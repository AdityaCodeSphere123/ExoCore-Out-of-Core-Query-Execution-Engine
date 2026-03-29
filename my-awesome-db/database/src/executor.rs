use anyhow::{bail, Result};
use common::query::{Query, QueryOp};
use db_config::DbContext;
use std::io::{BufRead, Write};

use crate::buffer_manager::BufferManager;
use crate::disk;
use crate::filter;
use crate::project;
use crate::operator::{ExecContext, Operator};

pub fn execute_query<RDisk, WDisk, WMon>(
    ctx: &DbContext,
    query: &Query,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    monitor_writer: &mut WMon,
    buffer_manager: &mut BufferManager,
) -> Result<()>
where
    RDisk: BufRead,
    WDisk: Write,
    WMon: Write,
{
    let mut operator = execute_op_tree(ctx, &query.root)?;
    let mut exec_ctx = ExecContext {
        db_ctx: ctx,
        disk_reader,
        disk_writer,
        buffer_manager,
    };

    monitor_writer.write_all(b"validate\n")?;
    while let Some(row) = operator.next(&mut exec_ctx)? {
        monitor_writer.write_all(row.to_pipe_string().as_bytes())?;
    }
    monitor_writer.write_all(b"!\n")?;
    monitor_writer.flush()?;

    Ok(())
}

fn execute_op_tree(
    ctx: &DbContext,
    op: &QueryOp,
) -> Result<Box<dyn Operator>> {
    match op {
        QueryOp::Scan(scan_data) => {
            let table_spec = disk::get_table_spec(ctx, &scan_data.table_id)?;
            let schema = disk::schema_from_table_spec(table_spec);
            Ok(Box::new(disk::ScanOperator::new(scan_data.table_id.clone(), schema)))
        }

        QueryOp::Filter(filter_data) => {
            let underlying = execute_op_tree(ctx, &filter_data.underlying)?;
            Ok(Box::new(filter::FilterOperator::new(underlying, filter_data.predicates.clone())))
        }

        QueryOp::Project(project_data) => {
            let underlying = execute_op_tree(ctx, &project_data.underlying)?;
            Ok(Box::new(project::ProjectOperator::new(underlying, &project_data.column_name_map)?))
        }

        _ => bail!("operator not implemented yet"),
    }
}