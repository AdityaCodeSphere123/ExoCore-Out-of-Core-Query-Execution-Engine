use anyhow::{bail, Result};
use common::query::{Query, QueryOp};
use db_config::DbContext;
use std::io::{BufRead, Write};

use crate::disk;

pub fn execute_query<RDisk, WDisk, WMon>(
    ctx: &DbContext,
    query: &Query,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    monitor_writer: &mut WMon,
) -> Result<()>
where
    RDisk: BufRead,
    WDisk: Write,
    WMon: Write,
{
    eprintln!("here1");
    match &query.root {
        QueryOp::Scan(scan_data) => {
            disk::scan_table(
                ctx,
                &scan_data.table_id,
                disk_reader,
                disk_writer,
                monitor_writer,
            )
        }
        _ => bail!("operator not implemented yet"),
    };
    eprintln!("here1");
    Ok(())
}