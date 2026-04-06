use anyhow::Result;
use common::query::{ComparisionValue, Predicate, Query, QueryOp, SortSpec};
use db_config::DbContext;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};

use crate::buffer_manager::BufferManager;
use crate::disk;
use crate::filter;
use crate::join;
use crate::operator::{ExecContext, Operator};
use crate::project;
use crate::row::RowSchema;
use crate::sort;
use crate::temp_storage::TempStorageManager;

pub fn execute_query<RDisk, WDisk, WMon>(
    ctx: &DbContext,
    query: &Query,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    monitor_writer: &mut WMon,
    buffer_manager: &mut BufferManager,
    temp_storage: &mut TempStorageManager,
    sort_run_bytes: usize,
) -> Result<()>
where
    RDisk: BufRead,
    WDisk: Write,
    WMon: Write,
{
    let mut operator = execute_op_tree(ctx, &query.root, None)?;
    let mut exec_ctx = ExecContext {
        db_ctx: ctx,
        disk_reader,
        disk_writer,
        buffer_manager,
        temp_storage,
        sort_run_bytes,
    };

    monitor_writer.write_all(b"validate\n")?;
    while let Some(row) = operator.next(&mut exec_ctx)? {
        monitor_writer.write_all(row.to_pipe_string().as_bytes())?;
    }
    monitor_writer.write_all(b"!\n")?;
    monitor_writer.flush()?;

    Ok(())
}

/// Build the operator tree, propagating `needed_above` (the set of column names
/// required by ancestors) downward so that scans can be wrapped in an early
/// projection that trims unused columns.
///
/// `needed_above = None` means "all columns are needed" (no pruning).
/// A `Project` node acts as a barrier: it replaces the needed set with exactly
/// the source columns it maps from.
fn execute_op_tree<'a>(
    ctx: &DbContext,
    op: &'a QueryOp,
    needed_above: Option<&HashSet<String>>,
) -> Result<Box<dyn Operator + 'a>> {
    match op {
        QueryOp::Scan(scan_data) => {
            let table_spec = disk::get_table_spec(ctx, &scan_data.table_id)?;
            let full_schema = disk::schema_from_table_spec(table_spec);

            // Late materialization: push column pruning into the scan itself
            // so unneeded columns are never decoded (skips string allocs, etc.).
            if let Some(needed) = needed_above {
                let needed_indices: Vec<usize> = full_schema
                    .column_names()
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| needed.contains(c.as_str()))
                    .map(|(i, _)| i)
                    .collect();

                if !needed_indices.is_empty() && needed_indices.len() < full_schema.len() {
                    let pruned_schema = RowSchema::new(
                        needed_indices
                            .iter()
                            .map(|&i| full_schema.column_names()[i].clone())
                            .collect(),
                    );
                    let total_cols = table_spec.column_specs.len();
                    return Ok(Box::new(
                        disk::ScanOperator::new(scan_data.table_id.clone(), pruned_schema)
                            .with_needed_columns(needed_indices, total_cols),
                    ));
                }
            }

            Ok(Box::new(disk::ScanOperator::new(
                scan_data.table_id.clone(),
                full_schema,
            )))
        }

        QueryOp::Filter(filter_data) => {
            let child_needed = add_predicate_columns(needed_above, &filter_data.predicates);

            // Rewrite Filter(Cross(...)) into a canonical SPJ-style left-deep plan:
            // 1. flatten the cross tree into base relations/subtrees,
            // 2. classify predicates by the relation(s) they touch,
            // 3. push single-relation predicates to the leaves,
            // 4. rebuild a connected join tree so predicates are applied as early as possible.
            if let Some(rewritten) = try_build_flattened_spj_plan(
                ctx,
                filter_data.underlying.as_ref(),
                &filter_data.predicates,
                child_needed.as_ref(),
            )? {
                return Ok(rewritten);
            }

            let underlying = execute_op_tree(ctx, &filter_data.underlying, child_needed.as_ref())?;
            Ok(Box::new(filter::FilterOperator::new(
                underlying,
                &filter_data.predicates,
            )?))
        }

        QueryOp::Cross(cross_data) => {
            let left = execute_op_tree(ctx, &cross_data.left, needed_above)?;
            let right = execute_op_tree(ctx, &cross_data.right, needed_above)?;
            join::build_join(left, right, &[])
        }

        QueryOp::Project(project_data) => {
            // Project is a barrier: only source columns are needed from child.
            let mut child_needed = HashSet::new();
            for (source, _alias) in &project_data.column_name_map {
                child_needed.insert(source.clone());
            }
            let underlying = execute_op_tree(ctx, &project_data.underlying, Some(&child_needed))?;
            Ok(Box::new(project::ProjectOperator::new(
                underlying,
                &project_data.column_name_map,
            )?))
        }

        QueryOp::Sort(sort_data) => {
            let child_needed = add_sort_columns(needed_above, &sort_data.sort_specs);
            let underlying = execute_op_tree(ctx, &sort_data.underlying, child_needed.as_ref())?;
            Ok(Box::new(sort::SortOperator::new(
                underlying,
                &sort_data.sort_specs,
            )?))
        }
    }
}

#[derive(Debug)]
struct FlattenedLeaf<'a> {
    index: usize,
    op: &'a QueryOp,
    schema: RowSchema,
    mask: u64,
}

#[derive(Debug, Clone)]
struct ClassifiedPredicate {
    predicate: Predicate,
    relation_mask: u64,
}

fn try_build_flattened_spj_plan<'a>(
    ctx: &DbContext,
    underlying: &'a QueryOp,
    predicates: &[Predicate],
    needed_above: Option<&HashSet<String>>,
) -> Result<Option<Box<dyn Operator + 'a>>> {
    let QueryOp::Cross(_) = underlying else {
        return Ok(None);
    };

    let mut raw_leaves = Vec::new();
    flatten_cross_inputs(underlying, &mut raw_leaves);

    if raw_leaves.len() <= 1 || raw_leaves.len() > 63 {
        return Ok(None);
    }

    let mut leaves = Vec::with_capacity(raw_leaves.len());
    let mut column_owner: HashMap<String, usize> = HashMap::new();

    for (idx, leaf_op) in raw_leaves.into_iter().enumerate() {
        let schema = logical_schema(ctx, leaf_op)?;

        // If column names are not globally unique, we cannot safely classify
        // predicates without a fuller name-resolution layer. Fall back.
        for col in schema.column_names() {
            if column_owner.insert(col.clone(), idx).is_some() {
                return Ok(None);
            }
        }

        leaves.push(FlattenedLeaf {
            index: idx,
            op: leaf_op,
            schema,
            mask: 1u64 << idx,
        });
    }

    let classified = match classify_predicates(predicates, &column_owner) {
        Some(v) => v,
        None => return Ok(None),
    };

    let mut leaf_needed: Vec<HashSet<String>> = vec![HashSet::new(); leaves.len()];

    // Columns needed by ancestors.
    if let Some(needed) = needed_above {
        for col in needed {
            let Some(&owner) = column_owner.get(col) else {
                return Ok(None);
            };
            leaf_needed[owner].insert(col.clone());
        }
    }

    // Columns needed to evaluate pushed-down filters and future joins.
    for pred in &classified {
        add_predicate_columns_to_leaf_sets(&mut leaf_needed, &column_owner, &pred.predicate)?;
    }

    let local_predicates = group_local_predicates(&classified, leaves.len());
    let join_order = choose_greedy_connected_order(&leaves, &classified);

    let mut built_ops: Vec<Option<Box<dyn Operator + 'a>>> = Vec::with_capacity(leaves.len());
    for leaf in &leaves {
        let needed = if leaf_needed[leaf.index].is_empty() {
            None
        } else {
            Some(&leaf_needed[leaf.index])
        };

        let mut op = execute_op_tree(ctx, leaf.op, needed)?;

        if !local_predicates[leaf.index].is_empty() {
            op = Box::new(filter::FilterOperator::new(op, &local_predicates[leaf.index])?);
        }

        built_ops.push(Some(op));
    }

    let mut placed = vec![false; classified.len()];
    for (pred_idx, pred) in classified.iter().enumerate() {
        if pred.relation_mask.count_ones() <= 1 {
            placed[pred_idx] = true;
        }
    }

    let first_idx = join_order[0];
    let mut current_mask = leaves[first_idx].mask;
    let mut current_op = built_ops[first_idx]
        .take()
        .expect("flattened leaf must exist");

    for &next_idx in join_order.iter().skip(1) {
        let next_mask = leaves[next_idx].mask;
        let available_mask = current_mask | next_mask;
        let mut join_preds = Vec::new();

        for (pred_idx, pred) in classified.iter().enumerate() {
            if placed[pred_idx] {
                continue;
            }

            if (pred.relation_mask & current_mask) != 0
                && (pred.relation_mask & next_mask) != 0
                && (pred.relation_mask & !available_mask) == 0
            {
                join_preds.push(pred.predicate.clone());
                placed[pred_idx] = true;
            }
        }

        let right_op = built_ops[next_idx]
            .take()
            .expect("flattened leaf must exist");
        current_op = join::build_join(current_op, right_op, &join_preds)?;
        current_mask = available_mask;
    }

    // Safety net: if anything was not attached during rebuilding, evaluate it once at the end.
    let residual_predicates: Vec<Predicate> = classified
        .iter()
        .enumerate()
        .filter(|(idx, _)| !placed[*idx])
        .map(|(_, pred)| pred.predicate.clone())
        .collect();

    if !residual_predicates.is_empty() {
        current_op = Box::new(filter::FilterOperator::new(current_op, &residual_predicates)?);
    }

    Ok(Some(current_op))
}

fn flatten_cross_inputs<'a>(op: &'a QueryOp, out: &mut Vec<&'a QueryOp>) {
    match op {
        QueryOp::Cross(cross_data) => {
            flatten_cross_inputs(&cross_data.left, out);
            flatten_cross_inputs(&cross_data.right, out);
        }
        _ => out.push(op),
    }
}

fn logical_schema(ctx: &DbContext, op: &QueryOp) -> Result<RowSchema> {
    match op {
        QueryOp::Scan(scan_data) => {
            let table_spec = disk::get_table_spec(ctx, &scan_data.table_id)?;
            Ok(disk::schema_from_table_spec(table_spec))
        }
        QueryOp::Filter(filter_data) => logical_schema(ctx, &filter_data.underlying),
        QueryOp::Sort(sort_data) => logical_schema(ctx, &sort_data.underlying),
        QueryOp::Project(project_data) => Ok(RowSchema::new(
            project_data
                .column_name_map
                .iter()
                .map(|(_, alias)| alias.clone())
                .collect(),
        )),
        QueryOp::Cross(cross_data) => {
            let left = logical_schema(ctx, &cross_data.left)?;
            let right = logical_schema(ctx, &cross_data.right)?;
            Ok(RowSchema::merge(&left, &right))
        }
    }
}

fn classify_predicates(
    predicates: &[Predicate],
    column_owner: &HashMap<String, usize>,
) -> Option<Vec<ClassifiedPredicate>> {
    let mut out = Vec::with_capacity(predicates.len());

    for pred in predicates {
        let mut mask = 0u64;

        let left_owner = *column_owner.get(&pred.column_name)?;
        mask |= 1u64 << left_owner;

        if let ComparisionValue::Column(other_col) = &pred.value {
            let right_owner = *column_owner.get(other_col)?;
            mask |= 1u64 << right_owner;
        }

        out.push(ClassifiedPredicate {
            predicate: pred.clone(),
            relation_mask: mask,
        });
    }

    Some(out)
}

fn add_predicate_columns_to_leaf_sets(
    leaf_needed: &mut [HashSet<String>],
    column_owner: &HashMap<String, usize>,
    pred: &Predicate,
) -> Result<()> {
    let left_owner = *column_owner
        .get(&pred.column_name)
        .expect("predicate column must have been classified");
    leaf_needed[left_owner].insert(pred.column_name.clone());

    if let ComparisionValue::Column(other_col) = &pred.value {
        let right_owner = *column_owner
            .get(other_col)
            .expect("predicate column must have been classified");
        leaf_needed[right_owner].insert(other_col.clone());
    }

    Ok(())
}

fn group_local_predicates(
    classified: &[ClassifiedPredicate],
    num_relations: usize,
) -> Vec<Vec<Predicate>> {
    let mut grouped = vec![Vec::new(); num_relations];

    for pred in classified {
        if pred.relation_mask.count_ones() == 1 {
            let rel_idx = pred.relation_mask.trailing_zeros() as usize;
            grouped[rel_idx].push(pred.predicate.clone());
        }
    }

    grouped
}

/// Choose a connected left-deep order using only lightweight heuristics.
///
/// This is intentionally not the full DP optimizer yet. The goal here is just
/// to stop following the syntactic cross-product order blindly once we have
/// already flattened the Filter(Cross(...)) block into relations + predicates.
fn choose_greedy_connected_order(
    leaves: &[FlattenedLeaf<'_>],
    classified: &[ClassifiedPredicate],
) -> Vec<usize> {
    let num_relations = leaves.len();
    let mut local_pred_count = vec![0usize; num_relations];
    let mut edge_count = vec![vec![0usize; num_relations]; num_relations];

    for pred in classified {
        match pred.relation_mask.count_ones() {
            1 => {
                let rel_idx = pred.relation_mask.trailing_zeros() as usize;
                local_pred_count[rel_idx] += 1;
            }
            2 => {
                let a = pred.relation_mask.trailing_zeros() as usize;
                let b_mask = pred.relation_mask & !(1u64 << a);
                let b = b_mask.trailing_zeros() as usize;
                edge_count[a][b] += 1;
                edge_count[b][a] += 1;
            }
            _ => {}
        }
    }

    let mut remaining: Vec<usize> = (0..num_relations).collect();
    remaining.sort_by_key(|&idx| {
        // More local predicates first; ties keep earlier relation order.
        (usize::MAX - local_pred_count[idx], idx)
    });

    let start = remaining[0];
    let mut order = vec![start];
    let mut current_mask = 1u64 << start;
    let mut used = vec![false; num_relations];
    used[start] = true;

    while order.len() < num_relations {
        let mut best_idx = None;
        let mut best_connected_edges = 0usize;
        let mut best_local_preds = 0usize;

        for candidate in 0..num_relations {
            if used[candidate] {
                continue;
            }

            let mut connected_edges = 0usize;
            for existing in 0..num_relations {
                if (current_mask & (1u64 << existing)) != 0 {
                    connected_edges += edge_count[candidate][existing];
                }
            }

            if connected_edges > 0 {
                let local = local_pred_count[candidate];
                if best_idx.is_none()
                    || connected_edges > best_connected_edges
                    || (connected_edges == best_connected_edges && local > best_local_preds)
                {
                    best_idx = Some(candidate);
                    best_connected_edges = connected_edges;
                    best_local_preds = local;
                }
            }
        }

        let chosen = if let Some(idx) = best_idx {
            idx
        } else {
            // Disconnected fallback: preserve original order among the remaining leaves.
            (0..num_relations).find(|&idx| !used[idx]).unwrap()
        };

        used[chosen] = true;
        current_mask |= 1u64 << chosen;
        order.push(chosen);
    }

    order
}

/// If `needed` is `Some`, extend it with columns referenced in the predicates.
/// If `needed` is `None` (no pruning), return `None` (still no pruning).
fn add_predicate_columns(
    needed: Option<&HashSet<String>>,
    predicates: &[Predicate],
) -> Option<HashSet<String>> {
    needed.map(|set| {
        let mut result = set.clone();
        for pred in predicates {
            result.insert(pred.column_name.clone());
            if let ComparisionValue::Column(c) = &pred.value {
                result.insert(c.clone());
            }
        }
        result
    })
}

/// If `needed` is `Some`, extend it with sort-key columns.
fn add_sort_columns(
    needed: Option<&HashSet<String>>,
    sort_specs: &[SortSpec],
) -> Option<HashSet<String>> {
    needed.map(|set| {
        let mut result = set.clone();
        for spec in sort_specs {
            result.insert(spec.column_name.clone());
        }
        result
    })
}
