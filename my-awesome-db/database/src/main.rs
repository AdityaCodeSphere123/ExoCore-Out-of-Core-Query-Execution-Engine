use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};
use crate::buffer_manager::BufferManager;

mod cli;
mod disk;
mod executor;
mod buffer;
mod buffer_manager;
mod row;
mod filter;
mod project;
mod io_setup;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    let mut disk_buf_reader = BufReader::new(disk_in);
    let mut monitor_buf_reader = BufReader::new(monitor_in);

    let mut input_line = String::new();

    // Read query from monitor
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(&input_line)?;
    eprintln!("Input query is: {:#?}", query);

    // Ask monitor for memory limit
    input_line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;
    eprintln!("Memory limit is set to {} MB", memory_limit_mb);
    let block_size = disk::get_block_size(&mut disk_buf_reader, &mut disk_out)?;

    let memory_limit_bytes = (memory_limit_mb as usize) * 1024 * 1024;
    let buffer_pool_bytes = memory_limit_bytes * 3 / 4;
    let capacity = std::cmp::max(1, buffer_pool_bytes / block_size);

    let mut buffer_manager = BufferManager::new(block_size, capacity)?;
    // Execute query
    executor::execute_query(
        &ctx,
        &query,
        &mut disk_buf_reader,
        &mut disk_out,
        &mut monitor_out,
        &mut buffer_manager,
    )?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}