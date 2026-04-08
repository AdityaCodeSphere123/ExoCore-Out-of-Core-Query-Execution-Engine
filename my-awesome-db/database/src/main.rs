use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::fs;
use std::io::{BufRead, BufReader, Write};

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};
use crate::buffer_manager::BufferManager;
use crate::temp_storage::TempStorageManager;

mod cli;
mod disk;
mod executor;
mod buffer;
mod buffer_manager;
mod row;
mod filter;
mod join;
mod project;
mod sort;
mod operator;
mod temp_storage;
mod io_setup;

#[derive(Debug, Default)]
struct ProcMemStatus {
    vm_peak_kb: Option<u64>,
    vm_size_kb: Option<u64>,
    vm_hwm_kb: Option<u64>,
    vm_rss_kb: Option<u64>,
}

fn parse_status_kb(line: &str, key: &str) -> Option<u64> {
    if !line.starts_with(key) {
        return None;
    }

    let mut parts = line.split_whitespace();
    let _label = parts.next()?;
    let value = parts.next()?.parse::<u64>().ok()?;
    Some(value)
}

fn read_proc_mem_status() -> Result<ProcMemStatus> {
    let text = fs::read_to_string("/proc/self/status")
        .context("failed to read /proc/self/status")?;

    let mut out = ProcMemStatus::default();

    for line in text.lines() {
        if out.vm_peak_kb.is_none() {
            out.vm_peak_kb = parse_status_kb(line, "VmPeak:");
        }
        if out.vm_size_kb.is_none() {
            out.vm_size_kb = parse_status_kb(line, "VmSize:");
        }
        if out.vm_hwm_kb.is_none() {
            out.vm_hwm_kb = parse_status_kb(line, "VmHWM:");
        }
        if out.vm_rss_kb.is_none() {
            out.vm_rss_kb = parse_status_kb(line, "VmRSS:");
        }
    }

    Ok(out)
}

fn fmt_opt_u64(v: Option<u64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_default()
}

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
    let stats_catalog = executor::StatsCatalog::from_config_path(cli_options.get_config_path())?;

    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    let mut disk_buf_reader = BufReader::new(disk_in);
    let mut monitor_buf_reader = BufReader::new(monitor_in);

    let mut input_line = String::new();

    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(&input_line)?;
    eprintln!("Input query is: {:#?}", query);

    input_line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;
    eprintln!("Memory limit is set to {} MB", memory_limit_mb);

    let block_size = disk::get_block_size(&mut disk_buf_reader, &mut disk_out)?;
    let memory_limit_bytes = (memory_limit_mb as usize) * 1024 * 1024;

    let capacity = 2;
    let mut buffer_manager = BufferManager::new(block_size, capacity)?;

    let usable_for_sort = memory_limit_bytes.saturating_sub(capacity * block_size);
    let sort_run_bytes = std::cmp::max(block_size, usable_for_sort * 4/5);
    let mut temp_storage = TempStorageManager::new(block_size)?;

    executor::execute_query(
        &ctx,
        &query,
        &mut disk_buf_reader,
        &mut disk_out,
        &mut monitor_out,
        &mut buffer_manager,
        &mut temp_storage,
        sort_run_bytes,
        &stats_catalog,
    )?;

    let mem = read_proc_mem_status()?;
    eprintln!(
        "DB_MEM_METRICS,vm_peak_kb={},vm_size_kb={},vm_hwm_kb={},vm_rss_kb={}",
        fmt_opt_u64(mem.vm_peak_kb),
        fmt_opt_u64(mem.vm_size_kb),
        fmt_opt_u64(mem.vm_hwm_kb),
        fmt_opt_u64(mem.vm_rss_kb),
    );

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
