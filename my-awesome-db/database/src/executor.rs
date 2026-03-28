use anyhow::{bail, Result};
use common::query::{Query, QueryOp};
use db_config::DbContext;
use std::io::{BufRead, Write};

use crate::buffer_manager::BufferManager;
use crate::disk;
use crate::filter;
use crate::project;
use crate::row::{Row, RowSchema};

type ExecResultSet = (RowSchema, Vec<Row>);

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
    let (_schema, rows) = execute_op(
        ctx,
        &query.root,
        disk_reader,
        disk_writer,
        buffer_manager,
    )?;

    monitor_writer.write_all(b"validate\n")?;
    for row in &rows {
        monitor_writer.write_all(row.to_pipe_string().as_bytes())?;
    }
    monitor_writer.write_all(b"!\n")?;
    monitor_writer.flush()?;

    Ok(())
}

fn execute_op<RDisk, WDisk>(
    ctx: &DbContext,
    op: &QueryOp,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    buffer_manager: &mut BufferManager,
) -> Result<ExecResultSet>
where
    RDisk: BufRead,
    WDisk: Write,
{
    match op {
        QueryOp::Scan(scan_data) => {
            disk::scan_table(ctx, &scan_data.table_id, disk_reader, disk_writer, buffer_manager)
        }

        QueryOp::Filter(filter_data) => {
            let (schema, rows) = execute_op(
                ctx,
                &filter_data.underlying.as_ref(),
                disk_reader,
                disk_writer,
                buffer_manager,
            )?;
            filter::apply_filter(&schema, rows, &filter_data.predicates)
        }

        QueryOp::Project(project_data) => {
            let (schema, rows) = execute_op(
                ctx,
                &project_data.underlying.as_ref(),
                disk_reader,
                disk_writer,
                buffer_manager,
            )?;
            project::apply_project(&schema, rows, &project_data.column_name_map)
        }

        _ => bail!("operator not implemented yet"),
    }
}