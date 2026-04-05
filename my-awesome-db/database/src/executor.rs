use anyhow::Result;
use common::query::{ComparisionValue, Predicate, Query, QueryOp, SortSpec};
use db_config::DbContext;
use std::collections::HashSet;
use std::io::{BufRead, Write};

use crate::buffer_manager::BufferManager;
use crate::disk;
use crate::filter;
use crate::join;
use crate::operator::{ExecContext, Operator};
use crate::project;
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
                    let pruned_schema = crate::row::RowSchema::new(
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
            // When a Filter sits directly on top of a Cross, treat the whole
            // thing as a join.
            if let QueryOp::Cross(cross_data) = filter_data.underlying.as_ref() {
                let child_needed =
                    add_predicate_columns(needed_above, &filter_data.predicates);
                let left =
                    execute_op_tree(ctx, &cross_data.left, child_needed.as_ref())?;
                let right =
                    execute_op_tree(ctx, &cross_data.right, child_needed.as_ref())?;
                return join::build_join(left, right, &filter_data.predicates);
            }
            let child_needed =
                add_predicate_columns(needed_above, &filter_data.predicates);
            let underlying =
                execute_op_tree(ctx, &filter_data.underlying, child_needed.as_ref())?;
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
            let underlying =
                execute_op_tree(ctx, &project_data.underlying, Some(&child_needed))?;
            Ok(Box::new(project::ProjectOperator::new(
                underlying,
                &project_data.column_name_map,
            )?))
        }

        QueryOp::Sort(sort_data) => {
            let child_needed =
                add_sort_columns(needed_above, &sort_data.sort_specs);
            let underlying =
                execute_op_tree(ctx, &sort_data.underlying, child_needed.as_ref())?;
            Ok(Box::new(sort::SortOperator::new(
                underlying,
                &sort_data.sort_specs,
            )?))
        }

    }
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
