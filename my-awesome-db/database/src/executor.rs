use anyhow::{anyhow, Result};
use common::query::{ComparisionOperator, ComparisionValue, Predicate, Query, QueryOp, SortSpec};
use db_config::DbContext;
use serde_json::Value;
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

#[derive(Debug, Clone, Default)]
pub struct StatsCatalog {
    tables: HashMap<String, TableStats>,
}

#[derive(Debug, Clone, Default)]
struct TableStats {
    row_count: Option<f64>,
    columns: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone, Default)]
struct ColumnStats {
    density: Option<f64>,
    cardinality_rows: Option<f64>,
    numeric_range: Option<(f64, f64)>,
    string_range: Option<(String, String)>,
    is_physically_ordered: Option<bool>,
}

#[derive(Debug, Clone)]
enum ScalarValue {
    Number(f64),
    Text(String),
}

impl StatsCatalog {
    pub fn from_config_path(path: &std::path::Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let json: Value = serde_json::from_str(&raw)?;
        Ok(Self::from_json_value(&json))
    }

    fn from_json_value(root: &Value) -> Self {
        let mut catalog = Self::default();
        visit_json_for_tables(root, &mut catalog);
        catalog
    }

    fn table(&self, table_id: &str) -> Option<&TableStats> {
        self.tables.get(table_id)
    }

    fn column(&self, table_id: &str, column_name: &str) -> Option<&ColumnStats> {
        self.table(table_id)?.columns.get(column_name)
    }

    fn row_count(&self, table_id: &str) -> Option<f64> {
        self.table(table_id)?.row_count
    }
}

fn visit_json_for_tables(value: &Value, catalog: &mut StatsCatalog) {
    match value {
        Value::Object(map) => {
            if let Some(columns_value) = map.get("column_specs") {
                if let Some(column_specs) = columns_value.as_array() {
                    let mut table_stats = TableStats::default();
                    for col_val in column_specs {
                        let Some(col_obj) = col_val.as_object() else {
                            continue;
                        };
                        let Some(col_name) = col_obj.get("column_name").and_then(Value::as_str) else {
                            continue;
                        };
                        let col_stats = parse_column_stats(col_obj.get("stats"));
                        if let Some(rows) = col_stats.cardinality_rows {
                            table_stats.row_count = Some(table_stats.row_count.unwrap_or(rows).max(rows));
                        }
                        table_stats.columns.insert(col_name.to_string(), col_stats);
                    }
                    if !table_stats.columns.is_empty() {
                        if let Some(name) = map.get("name").and_then(Value::as_str) {
                            catalog.tables.insert(name.to_string(), table_stats.clone());
                        }
                        if let Some(file_id) = map.get("file_id").and_then(Value::as_str) {
                            catalog.tables.insert(file_id.to_string(), table_stats.clone());
                        }
                    }
                }
            }
            for child in map.values() {
                visit_json_for_tables(child, catalog);
            }
        }
        Value::Array(arr) => {
            for child in arr {
                visit_json_for_tables(child, catalog);
            }
        }
        _ => {}
    }
}

fn parse_column_stats(stats_value: Option<&Value>) -> ColumnStats {
    let mut out = ColumnStats::default();
    let Some(stats_value) = stats_value else {
        return out;
    };

    match stats_value {
        Value::Array(entries) => {
            for entry in entries {
                apply_stat_entry(entry, &mut out);
            }
        }
        other => apply_stat_entry(other, &mut out),
    }

    out
}

fn apply_stat_entry(entry: &Value, out: &mut ColumnStats) {
    let Some(obj) = entry.as_object() else {
        return;
    };

    if let Some(v) = obj.get("CardinalityStat") {
        out.cardinality_rows = value_as_f64(v);
    }
    if let Some(v) = obj.get("DensityStat") {
        out.density = value_as_f64(v);
    }
    if let Some(v) = obj.get("IsPhysicallyOrdered") {
        out.is_physically_ordered = Some(match v {
            Value::Bool(b) => *b,
            Value::Null => true,
            _ => true,
        });
    }
    if let Some(v) = obj.get("RangeStat") {
        if let Some((lo, hi)) = parse_range_stat(v) {
            match (lo, hi) {
                (ScalarValue::Number(l), ScalarValue::Number(h)) => {
                    out.numeric_range = Some((l.min(h), l.max(h)));
                }
                (ScalarValue::Text(l), ScalarValue::Text(h)) => {
                    out.string_range = Some(if l <= h { (l, h) } else { (h, l) });
                }
                _ => {}
            }
        }
    }
}

fn parse_range_stat(value: &Value) -> Option<(ScalarValue, ScalarValue)> {
    match value {
        Value::Object(obj) => {
            let lower = obj.get("lower_bound").or_else(|| obj.get("lower"))?;
            let upper = obj.get("upper_bound").or_else(|| obj.get("upper"))?;
            Some((json_to_scalar(lower)?, json_to_scalar(upper)?))
        }
        Value::Array(arr) if arr.len() >= 2 => Some((json_to_scalar(&arr[0])?, json_to_scalar(&arr[1])?)),
        _ => None,
    }
}

fn json_to_scalar(value: &Value) -> Option<ScalarValue> {
    match value {
        Value::Number(n) => n.as_f64().map(ScalarValue::Number),
        Value::String(s) => Some(ScalarValue::Text(s.clone())),
        Value::Object(obj) if obj.len() == 1 => {
            let (k, v) = obj.iter().next()?;
            match k.as_str() {
                "I32" | "I64" | "F32" | "F64" => value_as_f64(v).map(ScalarValue::Number),
                "String" => v.as_str().map(|s| ScalarValue::Text(s.to_string())),
                _ => None,
            }
        }
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Object(obj) if obj.len() == 1 => {
            let (_, inner) = obj.iter().next()?;
            value_as_f64(inner)
        }
        _ => None,
    }
}

pub fn execute_query<RDisk, WDisk, WMon>(
    ctx: &DbContext,
    query: &Query,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    monitor_writer: &mut WMon,
    buffer_manager: &mut BufferManager,
    temp_storage: &mut TempStorageManager,
    sort_run_bytes: usize,
    stats_catalog: &StatsCatalog,
) -> Result<()>
where
    RDisk: BufRead,
    WDisk: Write,
    WMon: Write,
{
    let mut operator = execute_op_tree(ctx, &query.root, None, stats_catalog)?;
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

fn execute_op_tree<'a>(
    ctx: &DbContext,
    op: &'a QueryOp,
    needed_above: Option<&HashSet<String>>,
    stats_catalog: &StatsCatalog,
) -> Result<Box<dyn Operator + 'a>> {
    match op {
        QueryOp::Scan(scan_data) => build_scan_operator(ctx, scan_data, needed_above),

        QueryOp::Filter(filter_data) => {
            if let QueryOp::Cross(cross_data) = filter_data.underlying.as_ref() {
                if let Some(plan) = try_build_flattened_spj_plan(
                    ctx,
                    cross_data,
                    &filter_data.predicates,
                    needed_above,
                    stats_catalog,
                )? {
                    return Ok(plan);
                }

                let child_needed = add_predicate_columns(needed_above, &filter_data.predicates);
                let left = execute_op_tree(ctx, &cross_data.left, child_needed.as_ref(), stats_catalog)?;
                let right = execute_op_tree(ctx, &cross_data.right, child_needed.as_ref(), stats_catalog)?;
                return join::build_join(left, right, &filter_data.predicates);
            }

            let child_needed = add_predicate_columns(needed_above, &filter_data.predicates);
            let underlying = execute_op_tree(ctx, &filter_data.underlying, child_needed.as_ref(), stats_catalog)?;
            Ok(Box::new(filter::FilterOperator::new(
                underlying,
                &filter_data.predicates,
            )?))
        }

        QueryOp::Cross(cross_data) => {
            let left = execute_op_tree(ctx, &cross_data.left, needed_above, stats_catalog)?;
            let right = execute_op_tree(ctx, &cross_data.right, needed_above, stats_catalog)?;
            join::build_join(left, right, &[])
        }

        QueryOp::Project(project_data) => {
            let mut child_needed = HashSet::new();
            for (source, _alias) in &project_data.column_name_map {
                child_needed.insert(source.clone());
            }
            let underlying = execute_op_tree(ctx, &project_data.underlying, Some(&child_needed), stats_catalog)?;
            Ok(Box::new(project::ProjectOperator::new(
                underlying,
                &project_data.column_name_map,
            )?))
        }

        QueryOp::Sort(sort_data) => {
            let child_needed = add_sort_columns(needed_above, &sort_data.sort_specs);
            let underlying = execute_op_tree(ctx, &sort_data.underlying, child_needed.as_ref(), stats_catalog)?;
            Ok(Box::new(sort::SortOperator::new(
                underlying,
                &sort_data.sort_specs,
            )?))
        }
    }
}

fn build_scan_operator<'a>(
    ctx: &DbContext,
    scan_data: &'a common::query::ScanData,
    needed_above: Option<&HashSet<String>>,
) -> Result<Box<dyn Operator + 'a>> {
    let table_spec = disk::get_table_spec(ctx, &scan_data.table_id)?;
    let full_schema = disk::schema_from_table_spec(table_spec);

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

fn try_build_flattened_spj_plan<'a>(
    ctx: &DbContext,
    cross_root: &'a common::query::CrossData,
    predicates: &[Predicate],
    needed_above: Option<&HashSet<String>>,
    stats_catalog: &StatsCatalog,
) -> Result<Option<Box<dyn Operator + 'a>>> {
    let mut leaf_ops = Vec::new();
    flatten_cross_inputs(&cross_root.left, &mut leaf_ops);
    flatten_cross_inputs(&cross_root.right, &mut leaf_ops);

    if leaf_ops.len() < 2 {
        return Ok(None);
    }

    let mut leaves = Vec::with_capacity(leaf_ops.len());
    for (idx, leaf_op) in leaf_ops.into_iter().enumerate() {
        leaves.push(SpjLeaf {
            original_pos: idx,
            op: leaf_op,
            schema: logical_schema(ctx, leaf_op)?,
        });
    }

    let classification = match classify_predicates(&leaves, predicates)? {
        Some(c) => c,
        None => return Ok(None),
    };

    let needed_by_leaf = compute_needed_by_leaf(
        needed_above,
        &leaves,
        &classification.join_predicates,
        &classification.residual_predicates,
    );

    let order = if classification.join_predicates.is_empty() {
        (0..leaves.len()).collect()
    } else if let Some(order) = choose_left_deep_dp_order(
        &leaves,
        &classification.local_predicates,
        &classification.join_predicates,
        &classification.residual_predicates,
        needed_above,
        needed_by_leaf.as_ref(),
        stats_catalog,
    ) {
        order
    } else {
        (0..leaves.len()).collect()
    };

    let mut current_subset: Vec<usize> = Vec::new();
    let mut current_mask: usize = 0;
    let mut current_plan: Option<Box<dyn Operator + 'a>> = None;
    // Running row-count estimate for the accumulated left-side plan.  Used to
    // give BNLJ a hint about which side to spill (the smaller one).
    let mut current_rows: f64 = 1.0;

    for &leaf_idx in &order {
        // Estimate rows for this leaf after applying its local predicates.
        let leaf_base = estimate_base_rows_for_leaf(leaves[leaf_idx].op, stats_catalog)
            .unwrap_or(DEFAULT_BASE_REL_ROWS);
        let leaf_sel = estimate_local_selectivity_for_leaf(
            leaves[leaf_idx].op,
            &classification.local_predicates[leaf_idx],
            stats_catalog,
        );
        let leaf_rows = (leaf_base * leaf_sel).max(1.0);

        let leaf_needed = needed_by_leaf.as_ref().map(|sets| &sets[leaf_idx]);
        let leaf_plan = execute_single_relation_leaf(
            ctx,
            leaves[leaf_idx].op,
            &classification.local_predicates[leaf_idx],
            leaf_needed,
            stats_catalog,
        )?;

        let join_preds = if current_plan.is_some() {
            collect_join_preds_for_step(
                &classification.join_predicates,
                &current_subset,
                leaf_idx,
            )
        } else {
            Vec::new()
        };

        current_plan = Some(if let Some(left_plan) = current_plan {
            // Pass row-count hints so BNLJ can pick the smaller side to spill.
            join::build_join_hinted(left_plan, leaf_plan, &join_preds, current_rows, leaf_rows)?
        } else {
            leaf_plan
        });

        // Update the accumulated row estimate for the next join step.
        if current_subset.is_empty() {
            current_rows = leaf_rows;
        } else {
            let has_equi = join_preds
                .iter()
                .any(|p| matches!(p.operator, ComparisionOperator::EQ));
            let join_sel = if has_equi { EQ_JOIN_SELECTIVITY } else { OTHER_JOIN_SELECTIVITY };
            current_rows = (current_rows * leaf_rows * join_sel).max(1.0);
        }

        current_subset.push(leaf_idx);
        current_mask |= 1usize << leaf_idx;
        let keep_cols = compute_needed_columns_for_subset(
            current_mask,
            &leaves,
            &classification.join_predicates,
            &classification.residual_predicates,
            needed_above,
        );
        let plan = current_plan.take().expect("current plan must exist");
        current_plan = Some(trim_plan_to_needed(plan, &keep_cols)?);
    }

    let mut plan = current_plan.ok_or_else(|| anyhow!("failed to build SPJ plan"))?;
    if !classification.residual_predicates.is_empty() {
        plan = Box::new(filter::FilterOperator::new(
            plan,
            &classification.residual_predicates,
        )?);
    }

    Ok(Some(plan))
}

fn execute_single_relation_leaf<'a>(
    ctx: &DbContext,
    op: &'a QueryOp,
    local_predicates: &[Predicate],
    needed_above: Option<&HashSet<String>>,
    stats_catalog: &StatsCatalog,
) -> Result<Box<dyn Operator + 'a>> {
    match op {
        QueryOp::Scan(scan_data) => {
            let needed = add_predicate_columns(needed_above, local_predicates);
            let scan = build_scan_operator(ctx, scan_data, needed.as_ref())?;
            if local_predicates.is_empty() {
                Ok(scan)
            } else {
                Ok(Box::new(filter::FilterOperator::new(scan, local_predicates)?))
            }
        }

        QueryOp::Filter(filter_data) => {
            let mut merged = filter_data.predicates.clone();
            merged.extend_from_slice(local_predicates);
            execute_single_relation_leaf(ctx, &filter_data.underlying, &merged, needed_above, stats_catalog)
        }

        QueryOp::Sort(sort_data) => {
            let child_needed = add_sort_columns(needed_above, &sort_data.sort_specs);
            let child = execute_single_relation_leaf(
                ctx,
                &sort_data.underlying,
                local_predicates,
                child_needed.as_ref(),
                stats_catalog,
            )?;
            Ok(Box::new(sort::SortOperator::new(child, &sort_data.sort_specs)?))
        }

        QueryOp::Project(project_data) => {
            let remapped_needed = remap_needed_through_project(needed_above, &project_data.column_name_map);
            let remapped_preds = remap_predicates_through_project(local_predicates, &project_data.column_name_map)?;
            let child = execute_single_relation_leaf(
                ctx,
                &project_data.underlying,
                &remapped_preds,
                remapped_needed.as_ref(),
                stats_catalog,
            )?;
            Ok(Box::new(project::ProjectOperator::new(
                child,
                &project_data.column_name_map,
            )?))
        }

        QueryOp::Cross(_) => {
            let child_needed = add_predicate_columns(needed_above, local_predicates);
            let child = execute_op_tree(ctx, op, child_needed.as_ref(), stats_catalog)?;
            if local_predicates.is_empty() {
                Ok(child)
            } else {
                Ok(Box::new(filter::FilterOperator::new(child, local_predicates)?))
            }
        }
    }
}

fn remap_needed_through_project(
    needed_above: Option<&HashSet<String>>,
    column_name_map: &[(String, String)],
) -> Option<HashSet<String>> {
    needed_above.map(|needed| {
        let alias_to_source: HashMap<&str, &str> = column_name_map
            .iter()
            .map(|(source, alias)| (alias.as_str(), source.as_str()))
            .collect();

        let mut remapped = HashSet::new();
        for col in needed {
            if let Some(source) = alias_to_source.get(col.as_str()) {
                remapped.insert((*source).to_string());
            } else {
                remapped.insert(col.clone());
            }
        }
        remapped
    })
}

fn remap_predicates_through_project(
    predicates: &[Predicate],
    column_name_map: &[(String, String)],
) -> Result<Vec<Predicate>> {
    let alias_to_source: HashMap<&str, &str> = column_name_map
        .iter()
        .map(|(source, alias)| (alias.as_str(), source.as_str()))
        .collect();

    let mut remapped = Vec::with_capacity(predicates.len());
    for pred in predicates {
        let left = alias_to_source
            .get(pred.column_name.as_str())
            .copied()
            .unwrap_or(pred.column_name.as_str())
            .to_string();

        let value = match &pred.value {
            ComparisionValue::Column(c) => ComparisionValue::Column(
                alias_to_source
                    .get(c.as_str())
                    .copied()
                    .unwrap_or(c.as_str())
                    .to_string(),
            ),
            ComparisionValue::I32(v) => ComparisionValue::I32(*v),
            ComparisionValue::I64(v) => ComparisionValue::I64(*v),
            ComparisionValue::F32(v) => ComparisionValue::F32(*v),
            ComparisionValue::F64(v) => ComparisionValue::F64(*v),
            ComparisionValue::String(v) => ComparisionValue::String(v.clone()),
        };

        remapped.push(Predicate {
            column_name: left,
            operator: pred.operator.clone(),
            value,
        });
    }
    Ok(remapped)
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

#[derive(Clone)]
struct SpjLeaf<'a> {
    original_pos: usize,
    op: &'a QueryOp,
    schema: RowSchema,
}

#[derive(Clone)]
struct JoinPredicateInfo {
    left_leaf: usize,
    right_leaf: usize,
    predicate: Predicate,
    is_equi_column_join: bool,
}

struct PredicateClassification {
    local_predicates: Vec<Vec<Predicate>>,
    join_predicates: Vec<JoinPredicateInfo>,
    residual_predicates: Vec<Predicate>,
}

fn classify_predicates<'a>(
    leaves: &[SpjLeaf<'a>],
    predicates: &[Predicate],
) -> Result<Option<PredicateClassification>> {
    let mut local_predicates = vec![Vec::new(); leaves.len()];
    let mut join_predicates = Vec::new();
    let mut residual_predicates = Vec::new();

    for pred in predicates {
        // Count how many leaves own the left-hand column.
        let left_owners = count_column_owners(&pred.column_name, leaves);
        match left_owners {
            0 => {
                // Column is not present in any leaf — we cannot safely
                // classify this predicate.  Fall back to the simpler plan.
                return Ok(None);
            }
            1 => {
                let left_owner = unique_column_owner(&pred.column_name, leaves)
                    .expect("count was 1 so owner must exist");

                match &pred.value {
                    ComparisionValue::Column(other_col) => {
                        let right_owners = count_column_owners(other_col, leaves);
                        match right_owners {
                            0 => return Ok(None),
                            1 => {
                                let right_owner =
                                    unique_column_owner(other_col, leaves)
                                        .expect("count was 1");
                                if left_owner == right_owner {
                                    local_predicates[left_owner].push(pred.clone());
                                } else {
                                    let (left_leaf, right_leaf) =
                                        if left_owner < right_owner {
                                            (left_owner, right_owner)
                                        } else {
                                            (right_owner, left_owner)
                                        };
                                    join_predicates.push(JoinPredicateInfo {
                                        left_leaf,
                                        right_leaf,
                                        predicate: pred.clone(),
                                        is_equi_column_join: matches!(
                                            pred.operator,
                                            ComparisionOperator::EQ
                                        ),
                                    });
                                }
                            }
                            // Right column is ambiguous — can't push into
                            // either leaf; evaluate after all joins instead.
                            _ => residual_predicates.push(pred.clone()),
                        }
                    }
                    _ => local_predicates[left_owner].push(pred.clone()),
                }
            }
            // Left column is ambiguous — evaluate after all joins.
            _ => residual_predicates.push(pred.clone()),
        }
    }

    Ok(Some(PredicateClassification {
        local_predicates,
        join_predicates,
        residual_predicates,
    }))
}

/// Returns how many leaves contain `column_name` in their schema.
fn count_column_owners<'a>(column_name: &str, leaves: &[SpjLeaf<'a>]) -> usize {
    leaves.iter().filter(|l| l.schema.contains(column_name)).count()
}

fn unique_column_owner<'a>(column_name: &str, leaves: &[SpjLeaf<'a>]) -> Option<usize> {
    let mut owner = None;
    for (idx, leaf) in leaves.iter().enumerate() {
        if leaf.schema.contains(column_name) {
            if owner.is_some() {
                return None;
            }
            owner = Some(idx);
        }
    }
    owner
}

fn compute_needed_by_leaf<'a>(
    needed_above: Option<&HashSet<String>>,
    leaves: &[SpjLeaf<'a>],
    join_predicates: &[JoinPredicateInfo],
    residual_predicates: &[Predicate],
) -> Option<Vec<HashSet<String>>> {
    let mut needed_sets = if let Some(needed) = needed_above {
        let mut per_leaf = vec![HashSet::new(); leaves.len()];
        for col in needed {
            for (leaf_idx, leaf) in leaves.iter().enumerate() {
                if leaf.schema.contains(col) {
                    per_leaf[leaf_idx].insert(col.clone());
                }
            }
        }
        per_leaf
    } else {
        return None;
    };

    for info in join_predicates {
        add_predicate_to_leaf_set(&mut needed_sets[info.left_leaf], &info.predicate, &leaves[info.left_leaf].schema);
        add_predicate_to_leaf_set(&mut needed_sets[info.right_leaf], &info.predicate, &leaves[info.right_leaf].schema);
    }
    for pred in residual_predicates {
        for (leaf_idx, leaf) in leaves.iter().enumerate() {
            add_predicate_to_leaf_set(&mut needed_sets[leaf_idx], pred, &leaf.schema);
        }
    }

    Some(needed_sets)
}

fn add_predicate_to_leaf_set(needed: &mut HashSet<String>, pred: &Predicate, schema: &RowSchema) {
    if schema.contains(&pred.column_name) {
        needed.insert(pred.column_name.clone());
    }
    if let ComparisionValue::Column(c) = &pred.value {
        if schema.contains(c) {
            needed.insert(c.clone());
        }
    }
}

fn collect_join_preds_for_step(
    join_predicates: &[JoinPredicateInfo],
    current_subset: &[usize],
    new_leaf: usize,
) -> Vec<Predicate> {
    let current: HashSet<usize> = current_subset.iter().copied().collect();
    let mut preds = Vec::new();
    for info in join_predicates {
        let touches_new = info.left_leaf == new_leaf || info.right_leaf == new_leaf;
        let other = if info.left_leaf == new_leaf {
            info.right_leaf
        } else if info.right_leaf == new_leaf {
            info.left_leaf
        } else {
            continue;
        };
        if touches_new && current.contains(&other) {
            preds.push(info.predicate.clone());
        }
    }
    preds
}

fn compute_needed_columns_for_subset<'a>(
    subset_mask: usize,
    leaves: &[SpjLeaf<'a>],
    join_predicates: &[JoinPredicateInfo],
    residual_predicates: &[Predicate],
    final_needed: Option<&HashSet<String>>,
) -> HashSet<String> {
    let mut needed = HashSet::new();

    if let Some(final_needed) = final_needed {
        for col in final_needed {
            if subset_contains_column(subset_mask, leaves, col) {
                needed.insert(col.clone());
            }
        }
    } else {
        for (leaf_idx, leaf) in leaves.iter().enumerate() {
            if (subset_mask & (1usize << leaf_idx)) == 0 {
                continue;
            }
            for col in leaf.schema.column_names() {
                needed.insert(col.clone());
            }
        }
    }

    for info in join_predicates {
        let left_in = (subset_mask & (1usize << info.left_leaf)) != 0;
        let right_in = (subset_mask & (1usize << info.right_leaf)) != 0;

        if left_in ^ right_in {
            needed.insert(info.predicate.column_name.clone());
            if let ComparisionValue::Column(c) = &info.predicate.value {
                needed.insert(c.clone());
            }
        }
    }

    for pred in residual_predicates {
        if subset_contains_column(subset_mask, leaves, &pred.column_name) {
            needed.insert(pred.column_name.clone());
        }
        if let ComparisionValue::Column(c) = &pred.value {
            if subset_contains_column(subset_mask, leaves, c) {
                needed.insert(c.clone());
            }
        }
    }

    needed
}

fn subset_contains_column<'a>(subset_mask: usize, leaves: &[SpjLeaf<'a>], column_name: &str) -> bool {
    for (leaf_idx, leaf) in leaves.iter().enumerate() {
        if (subset_mask & (1usize << leaf_idx)) != 0 && leaf.schema.contains(column_name) {
            return true;
        }
    }
    false
}

fn trim_plan_to_needed<'a>(
    plan: Box<dyn Operator + 'a>,
    needed: &HashSet<String>,
) -> Result<Box<dyn Operator + 'a>> {
    let schema = plan.schema().clone();
    if needed.is_empty() || needed.len() >= schema.len() {
        return Ok(plan);
    }

    let column_name_map: Vec<(String, String)> = schema
        .column_names()
        .iter()
        .filter(|col| needed.contains(col.as_str()))
        .map(|col| (col.clone(), col.clone()))
        .collect();

    if column_name_map.is_empty() || column_name_map.len() == schema.len() {
        Ok(plan)
    } else {
        Ok(Box::new(project::ProjectOperator::new(plan, &column_name_map)?))
    }
}

#[derive(Clone, Copy, Debug)]
struct PlanStats {
    rows: f64,
    width_bytes: f64,
    pages: f64,
    cost: f64,
}

#[derive(Clone)]
struct DpPlan {
    order: Vec<usize>,
    stats: PlanStats,
}

fn choose_left_deep_dp_order<'a>(
    leaves: &[SpjLeaf<'a>],
    local_predicates: &[Vec<Predicate>],
    join_predicates: &[JoinPredicateInfo],
    residual_predicates: &[Predicate],
    final_needed: Option<&HashSet<String>>,
    needed_by_leaf: Option<&Vec<HashSet<String>>>,
    stats_catalog: &StatsCatalog,
) -> Option<Vec<usize>> {
    let n = leaves.len();
    if n == 0 || n > 20 {
        return None;
    }

    let full_mask = (1usize << n) - 1;
    let mut dp: Vec<Option<DpPlan>> = vec![None; 1usize << n];
    let leaf_stats: Vec<PlanStats> = (0..n)
        .map(|leaf_idx| {
            let subset_width_bytes = estimate_subset_width_bytes(
                1usize << leaf_idx,
                leaves,
                join_predicates,
                residual_predicates,
                final_needed,
            );
            estimate_leaf_access_stats(
                leaves,
                local_predicates,
                needed_by_leaf,
                leaf_idx,
                subset_width_bytes,
                stats_catalog,
            )
        })
        .collect();

    for leaf_idx in 0..n {
        let mask = 1usize << leaf_idx;
        dp[mask] = Some(DpPlan {
            order: vec![leaf_idx],
            stats: leaf_stats[leaf_idx],
        });
    }

    for mask in 1usize..=full_mask {
        let prev_plan = match dp[mask].clone() {
            Some(plan) => plan,
            None => continue,
        };
        if mask == full_mask {
            continue;
        }

        for next_leaf in 0..n {
            if (mask & (1usize << next_leaf)) != 0 {
                continue;
            }

            let step_preds = step_join_predicates(mask, next_leaf, join_predicates);
            if step_preds.is_empty() {
                continue;
            }

            let next_mask = mask | (1usize << next_leaf);
            let mut next_order = prev_plan.order.clone();
            next_order.push(next_leaf);
            let subset_width_bytes = estimate_subset_width_bytes(
                next_mask,
                leaves,
                join_predicates,
                residual_predicates,
                final_needed,
            );
            let next_stats = estimate_join_step(
                prev_plan.stats,
                leaf_stats[next_leaf],
                &step_preds,
                leaves,
                stats_catalog,
                subset_width_bytes,
                leaves[next_leaf].original_pos,
            );

            let should_replace = match &dp[next_mask] {
                None => true,
                Some(existing) => better_plan(next_stats, existing.stats),
            };

            if should_replace {
                dp[next_mask] = Some(DpPlan {
                    order: next_order,
                    stats: next_stats,
                });
            }
        }
    }

    dp[full_mask].as_ref().map(|plan| plan.order.clone())
}

fn better_plan(new_stats: PlanStats, old_stats: PlanStats) -> bool {
    const EPS: f64 = 1e-9;
    if new_stats.cost + EPS < old_stats.cost {
        return true;
    }
    if old_stats.cost + EPS < new_stats.cost {
        return false;
    }

    if new_stats.pages + EPS < old_stats.pages {
        return true;
    }
    if old_stats.pages + EPS < new_stats.pages {
        return false;
    }

    if new_stats.rows + EPS < old_stats.rows {
        return true;
    }
    false
}

fn estimate_leaf_access_stats<'a>(
    leaves: &[SpjLeaf<'a>],
    local_predicates: &[Vec<Predicate>],
    needed_by_leaf: Option<&Vec<HashSet<String>>>,
    leaf_idx: usize,
    subset_width_bytes: f64,
    stats_catalog: &StatsCatalog,
) -> PlanStats {
    let width_bytes = subset_width_bytes;
    let base_rows = estimate_base_rows_for_leaf(leaves[leaf_idx].op, stats_catalog).unwrap_or(DEFAULT_BASE_REL_ROWS);
    let selectivity = estimate_local_selectivity_for_leaf(leaves[leaf_idx].op, &local_predicates[leaf_idx], stats_catalog);
    let rows = (base_rows * selectivity).max(1.0);

    // If a range predicate targets a physically-ordered column we can skip
    // blocks that fall outside the predicate range.  Model this by scaling the
    // page estimate by the fraction of blocks we actually need to read (the
    // range selectivity).  This gives the join-order optimizer a realistic
    // picture of how cheap such a scan really is.
    let scan_fraction = ordered_scan_fraction(
        leaves[leaf_idx].op,
        &local_predicates[leaf_idx],
        stats_catalog,
    );
    let pages = (estimate_pages(rows, width_bytes) * scan_fraction).max(1.0);

    let _ = (leaves, needed_by_leaf); // width already comes from subset_width_bytes.

    PlanStats {
        rows,
        width_bytes,
        pages,
        cost: pages,
    }
}

/// Returns the fraction of blocks that must be read when one or more range
/// predicates target a physically-ordered column.  Returns 1.0 (full scan)
/// when no such predicate exists.
///
/// When `IsPhysicallyOrdered` is true the data is stored sorted on that
/// column, so a range predicate `col > X` only needs to touch the tail of the
/// file rather than every block.  We use the range selectivity (derived from
/// `RangeStat`) as a proxy for this fraction.  The minimum over all ordered
/// range predicates is used when several such predicates are present.
fn ordered_scan_fraction(
    op: &QueryOp,
    predicates: &[Predicate],
    stats_catalog: &StatsCatalog,
) -> f64 {
    let table_id = match leaf_base_table_id(op) {
        Some(id) => id,
        None => return 1.0,
    };

    let mut best: f64 = 1.0;
    for pred in predicates {
        let col_stats = match stats_catalog.column(table_id, &pred.column_name) {
            Some(s) => s,
            None => continue,
        };
        if col_stats.is_physically_ordered != Some(true) {
            continue;
        }
        let is_range = matches!(
            pred.operator,
            ComparisionOperator::GT
                | ComparisionOperator::GTE
                | ComparisionOperator::LT
                | ComparisionOperator::LTE
        );
        if !is_range {
            continue;
        }
        let frac = estimate_range_selectivity(Some(col_stats), pred)
            .unwrap_or(RANGE_PRED_SELECTIVITY);
        if frac < best {
            best = frac;
        }
    }
    best
}

fn estimate_subset_width_bytes<'a>(
    subset_mask: usize,
    leaves: &[SpjLeaf<'a>],
    join_predicates: &[JoinPredicateInfo],
    residual_predicates: &[Predicate],
    final_needed: Option<&HashSet<String>>,
) -> f64 {
    let keep_cols = compute_needed_columns_for_subset(
        subset_mask,
        leaves,
        join_predicates,
        residual_predicates,
        final_needed,
    );
    let width_cols = keep_cols.len().max(1);
    estimate_width_bytes(width_cols)
}

fn estimate_join_step<'a>(
    left: PlanStats,
    right: PlanStats,
    step_preds: &[&JoinPredicateInfo],
    leaves: &[SpjLeaf<'a>],
    stats_catalog: &StatsCatalog,
    output_width: f64,
    right_original_pos: usize,
) -> PlanStats {
    let output_rows = estimate_join_rows(left.rows, right.rows, step_preds, leaves, stats_catalog);
    let output_pages = estimate_pages(output_rows, output_width).max(1.0);

    let has_equi = step_preds.iter().any(|pred| pred.is_equi_column_join);
    let join_work = if has_equi {
        left.pages + right.pages + output_pages
    } else {
        left.pages + (left.pages * right.pages) + output_pages
    };

    let _ = right_original_pos; // unused after tie_break removal

    PlanStats {
        rows: output_rows,
        width_bytes: output_width,
        pages: output_pages,
        cost: left.cost + right.cost + join_work,
    }
}

fn estimate_base_rows_for_leaf(op: &QueryOp, stats_catalog: &StatsCatalog) -> Option<f64> {
    let table_id = leaf_base_table_id(op)?;
    stats_catalog.row_count(table_id)
}

fn leaf_base_table_id<'a>(op: &'a QueryOp) -> Option<&'a str> {
    match op {
        QueryOp::Scan(scan_data) => Some(scan_data.table_id.as_str()),
        QueryOp::Filter(filter_data) => leaf_base_table_id(&filter_data.underlying),
        QueryOp::Sort(sort_data) => leaf_base_table_id(&sort_data.underlying),
        QueryOp::Project(project_data) => leaf_base_table_id(&project_data.underlying),
        QueryOp::Cross(_) => None,
    }
}

fn estimate_local_selectivity_for_leaf(
    op: &QueryOp,
    extra_predicates: &[Predicate],
    stats_catalog: &StatsCatalog,
) -> f64 {
    if let Some((table_id, predicates)) = extract_scan_and_source_predicates(op, extra_predicates.to_vec()) {
        estimate_local_selectivity_on_table(stats_catalog, &table_id, &predicates)
    } else {
        estimate_local_selectivity(extra_predicates)
    }
}

fn extract_scan_and_source_predicates(op: &QueryOp, mut predicates: Vec<Predicate>) -> Option<(String, Vec<Predicate>)> {
    match op {
        QueryOp::Scan(scan_data) => Some((scan_data.table_id.clone(), predicates)),
        QueryOp::Filter(filter_data) => {
            predicates.extend(filter_data.predicates.clone());
            extract_scan_and_source_predicates(&filter_data.underlying, predicates)
        }
        QueryOp::Sort(sort_data) => extract_scan_and_source_predicates(&sort_data.underlying, predicates),
        QueryOp::Project(project_data) => {
            let remapped = remap_predicates_through_project(&predicates, &project_data.column_name_map).ok()?;
            extract_scan_and_source_predicates(&project_data.underlying, remapped)
        }
        QueryOp::Cross(_) => None,
    }
}

fn estimate_local_selectivity_on_table(
    stats_catalog: &StatsCatalog,
    table_id: &str,
    predicates: &[Predicate],
) -> f64 {
    let mut sel: f64 = 1.0;
    for pred in predicates {
        sel *= estimate_predicate_selectivity(stats_catalog, table_id, pred);
    }
    sel.clamp(MIN_SELECTIVITY, 1.0_f64)
}

fn estimate_predicate_selectivity(
    stats_catalog: &StatsCatalog,
    table_id: &str,
    pred: &Predicate,
) -> f64 {
    match &pred.value {
        ComparisionValue::Column(_) => estimate_local_selectivity(std::slice::from_ref(pred)),
        _ => {
            let row_count = stats_catalog.row_count(table_id).unwrap_or(DEFAULT_BASE_REL_ROWS);
            let col_stats = stats_catalog.column(table_id, &pred.column_name);
            match pred.operator {
                ComparisionOperator::EQ => estimate_eq_literal_selectivity(col_stats, row_count),
                ComparisionOperator::NE => (1.0 - estimate_eq_literal_selectivity(col_stats, row_count))
                    .clamp(MIN_SELECTIVITY, 1.0_f64),
                ComparisionOperator::GT
                | ComparisionOperator::GTE
                | ComparisionOperator::LT
                | ComparisionOperator::LTE => estimate_range_selectivity(col_stats, pred)
                    .unwrap_or(RANGE_PRED_SELECTIVITY),
            }
        }
    }
}

fn estimate_eq_literal_selectivity(col_stats: Option<&ColumnStats>, row_count: f64) -> f64 {
    if let Some(stats) = col_stats {
        if let Some(density) = stats.density {
            // DensityStat = fraction of rows with a unique value, so
            // NDV = row_count * density.
            let ndv = (row_count * density).max(1.0);
            return (1.0 / ndv).clamp(MIN_SELECTIVITY, 1.0_f64);
        }
    }
    // No DensityStat available.  The flat 10% default is far too high for any
    // large table (e.g. TPC-H lineitem has ~6M rows; 10% = 600k matches for an
    // equality predicate, which is absurd).  Use sqrt(row_count) as a rough NDV
    // estimate — this gives ~1/2449 for lineitem, orders of magnitude more
    // realistic and produces much better join-order decisions.
    let ndv = row_count.sqrt().max(1.0);
    (1.0 / ndv).clamp(MIN_SELECTIVITY, 1.0_f64)
}

fn estimate_range_selectivity(col_stats: Option<&ColumnStats>, pred: &Predicate) -> Option<f64> {
    let stats = col_stats?;
    match (
        stats.numeric_range,
        stats.string_range.as_ref(),
        comparison_value_to_scalar(&pred.value),
    ) {
        (Some((lo, hi)), _, Some(ScalarValue::Number(v))) => {
            let span = (hi - lo).abs();
            if span <= f64::EPSILON {
                return Some(EQ_PRED_SELECTIVITY);
            }
            let frac = match pred.operator {
                ComparisionOperator::LT | ComparisionOperator::LTE => ((v - lo) / span).clamp(0.0, 1.0),
                ComparisionOperator::GT | ComparisionOperator::GTE => ((hi - v) / span).clamp(0.0, 1.0),
                _ => return None,
            };
            Some(frac.clamp(MIN_SELECTIVITY, 1.0_f64))
        }
        (_, Some((lo, hi)), Some(ScalarValue::Text(v))) => {
            if lo == hi {
                return Some(EQ_PRED_SELECTIVITY);
            }
            let frac: f64 = match pred.operator {
                ComparisionOperator::LT | ComparisionOperator::LTE => {
                    if &v <= lo {
                        0.0_f64
                    } else if &v >= hi {
                        1.0_f64
                    } else {
                        0.5_f64
                    }
                }
                ComparisionOperator::GT | ComparisionOperator::GTE => {
                    if &v <= lo {
                        1.0_f64
                    } else if &v >= hi {
                        0.0_f64
                    } else {
                        0.5_f64
                    }
                }
                _ => return None,
            };
            Some(frac.clamp(MIN_SELECTIVITY, 1.0_f64))
        }
        _ => None,
    }
}

fn comparison_value_to_scalar(value: &ComparisionValue) -> Option<ScalarValue> {
    match value {
        ComparisionValue::I32(v) => Some(ScalarValue::Number(*v as f64)),
        ComparisionValue::I64(v) => Some(ScalarValue::Number(*v as f64)),
        ComparisionValue::F32(v) => Some(ScalarValue::Number(*v as f64)),
        ComparisionValue::F64(v) => Some(ScalarValue::Number(*v)),
        ComparisionValue::String(v) => Some(ScalarValue::Text(v.clone())),
        ComparisionValue::Column(_) => None,
    }
}

fn estimate_local_selectivity(predicates: &[Predicate]) -> f64 {
    let mut sel: f64 = 1.0;

    for pred in predicates {
        sel *= match pred.operator {
            ComparisionOperator::EQ => EQ_PRED_SELECTIVITY,
            ComparisionOperator::NE => NE_PRED_SELECTIVITY,
            ComparisionOperator::GT
            | ComparisionOperator::GTE
            | ComparisionOperator::LT
            | ComparisionOperator::LTE => RANGE_PRED_SELECTIVITY,
        };
    }

    sel.clamp(MIN_SELECTIVITY, 1.0_f64)
}

fn step_join_predicates<'a>(
    subset_mask: usize,
    next_leaf: usize,
    join_predicates: &'a [JoinPredicateInfo],
) -> Vec<&'a JoinPredicateInfo> {
    let mut preds = Vec::new();
    for info in join_predicates {
        let touches_next = info.left_leaf == next_leaf || info.right_leaf == next_leaf;
        if !touches_next {
            continue;
        }
        let other = if info.left_leaf == next_leaf {
            info.right_leaf
        } else {
            info.left_leaf
        };
        if (subset_mask & (1usize << other)) != 0 {
            preds.push(info);
        }
    }
    preds
}

fn estimate_join_rows<'a>(
    left_rows: f64,
    right_rows: f64,
    step_preds: &[&JoinPredicateInfo],
    leaves: &[SpjLeaf<'a>],
    stats_catalog: &StatsCatalog,
) -> f64 {
    let mut sel: f64 = 1.0;

    for pred in step_preds {
        if pred.is_equi_column_join {
            sel *= estimate_equi_join_selectivity(pred, leaves, stats_catalog)
                .unwrap_or(EQ_JOIN_SELECTIVITY);
        } else {
            sel *= OTHER_JOIN_SELECTIVITY;
        }
    }

    (left_rows * right_rows * sel).max(1.0_f64)
}

fn estimate_equi_join_selectivity<'a>(
    info: &JoinPredicateInfo,
    leaves: &[SpjLeaf<'a>],
    stats_catalog: &StatsCatalog,
) -> Option<f64> {
    let left_col = resolve_column_in_leaf(leaves[info.left_leaf].op, &info.predicate.column_name)?;
    let right_name = match &info.predicate.value {
        ComparisionValue::Column(c) => c.as_str(),
        _ => return None,
    };
    let right_col = resolve_column_in_leaf(leaves[info.right_leaf].op, right_name)?;

    let left_rows = stats_catalog.row_count(&left_col.0).unwrap_or(DEFAULT_BASE_REL_ROWS);
    let right_rows = stats_catalog.row_count(&right_col.0).unwrap_or(DEFAULT_BASE_REL_ROWS);
    let left_density = stats_catalog.column(&left_col.0, &left_col.1)?.density?;
    let right_density = stats_catalog.column(&right_col.0, &right_col.1)?.density?;

    let left_ndv = (left_rows * left_density).max(1.0);
    let right_ndv = (right_rows * right_density).max(1.0);
    Some((1.0 / left_ndv.max(right_ndv)).clamp(MIN_SELECTIVITY, 1.0_f64))
}

fn resolve_column_in_leaf(op: &QueryOp, column_name: &str) -> Option<(String, String)> {
    match op {
        QueryOp::Scan(scan_data) => Some((scan_data.table_id.clone(), column_name.to_string())),
        QueryOp::Filter(filter_data) => resolve_column_in_leaf(&filter_data.underlying, column_name),
        QueryOp::Sort(sort_data) => resolve_column_in_leaf(&sort_data.underlying, column_name),
        QueryOp::Project(project_data) => {
            let source = project_data
                .column_name_map
                .iter()
                .find(|(_, alias)| alias == column_name)
                .map(|(source, _)| source.as_str())
                .unwrap_or(column_name);
            resolve_column_in_leaf(&project_data.underlying, source)
        }
        QueryOp::Cross(_) => None,
    }
}

fn estimate_width_bytes(width_cols: usize) -> f64 {
    ROW_OVERHEAD_BYTES + AVG_COL_WIDTH_BYTES * width_cols as f64
}

fn estimate_pages(rows: f64, width_bytes: f64) -> f64 {
    (rows * width_bytes / DEFAULT_BLOCK_BYTES).ceil().max(1.0)
}

const DEFAULT_BASE_REL_ROWS: f64 = 10_000.0;
const AVG_COL_WIDTH_BYTES: f64 = 24.0;
const ROW_OVERHEAD_BYTES: f64 = 8.0;
const DEFAULT_BLOCK_BYTES: f64 = 4096.0;
const MIN_SELECTIVITY: f64 = 0.0001;
const EQ_PRED_SELECTIVITY: f64 = 0.10;
const NE_PRED_SELECTIVITY: f64 = 0.90;
const RANGE_PRED_SELECTIVITY: f64 = 0.33;
const EQ_JOIN_SELECTIVITY: f64 = 0.001;
const OTHER_JOIN_SELECTIVITY: f64 = 0.10;

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
