use anyhow::{Context, Result, bail};
use clap::Parser;
use libc::{rlimit64, setrlimit64};
use monitor_config::{
    MonitorConfig,
    monitor_config::{DatabaseConfig, QueryConfig},
};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, PipeReader, PipeWriter, Read, Write, pipe},
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use crate::{
    cli::CliOptions,
    fd_mapper::{FdMapping, remap_fds},
};

mod cli;
mod fd_mapper;

const MAX_COMMAND_LENGTH: u64 = 1024;

#[derive(Debug, Default, Clone)]
struct DiskMetricsRow {
    total_reads: u64,
    total_writes: u64,
    total_blocks_processed: u64,
    total_cylinders_traveled: u64,
    total_io_time_us: u64,
    total_seek_time_us: u64,
    total_rotational_latency_us: u64,
    total_transfer_time_us: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct DbMemMetricsRow {
    vm_peak_kb: Option<u64>,
    vm_size_kb: Option<u64>,
    vm_hwm_kb: Option<u64>,
    vm_rss_kb: Option<u64>,
}

fn setup_disk_process(monitor_config: &MonitorConfig) -> Result<(Child, PipeReader, PipeWriter)> {
    let (disk_outbound_reader, disk_outbound_writer) = pipe()?;
    let (disk_inbound_reader, disk_inbound_writer) = pipe()?;

    let disk_prog = &monitor_config.get_disk_config().disk_prog;
    let disk_prog_config = &monitor_config.get_disk_config().disk_prog_config;

    let disk_process = Command::new(disk_prog)
        .arg("--config")
        .arg(disk_prog_config)
        .stdin(disk_inbound_reader)
        .stdout(disk_outbound_writer)
        .stderr(Stdio::piped())
        .spawn()?;

    Ok((disk_process, disk_outbound_reader, disk_inbound_writer))
}

fn setup_db_process(
    database_config: &DatabaseConfig,
    query_config: &QueryConfig,
    disk_outbound_reader: PipeReader,
    disk_inbound_writer: PipeWriter,
) -> Result<(Child, PipeReader, PipeWriter)> {
    let (monitor_to_db_reader, monitor_to_db_writer) = pipe()?;
    let (db_to_monitor_reader, db_to_monitor_writer) = pipe()?;

    let db_prog = &database_config.database_prog;
    let db_prog_config = &database_config.database_prog_config;

    let mut db_process = Command::new(db_prog);
    db_process
        .arg("--config")
        .arg(db_prog_config)
        .stderr(Stdio::piped());

    let memory_limit = query_config.memory_limit_mb * 1024 * 1024;

    unsafe {
        db_process.pre_exec(move || {
            remap_fds(&vec![
                FdMapping::new(disk_outbound_reader.as_raw_fd(), 3, false),
                FdMapping::new(disk_inbound_writer.as_raw_fd(), 4, false),
                FdMapping::new(monitor_to_db_reader.as_raw_fd(), 5, false),
                FdMapping::new(db_to_monitor_writer.as_raw_fd(), 6, false),
            ]);

            let mut rlimit = rlimit64 {
                rlim_cur: memory_limit,
                rlim_max: memory_limit,
            };

            if setrlimit64(libc::RLIMIT_AS, &rlimit) != 0 {
                panic!("Unable to set memory limit");
            }

            if setrlimit64(libc::RLIMIT_STACK, &rlimit) != 0 {
                panic!("Unable to set stack limit");
            }

            rlimit.rlim_cur = 0;
            rlimit.rlim_max = 0;
            if setrlimit64(libc::RLIMIT_FSIZE, &rlimit) != 0 {
                panic!("Unable to set max file size limit");
            }

            rlimit.rlim_cur = 1;
            rlimit.rlim_max = 1;
            if setrlimit64(libc::RLIMIT_NPROC, &rlimit) != 0 {
                panic!("Unable to set max processes limit");
            }

            Ok(())
        });
    }

    let db_process_child = db_process.spawn()?;

    Ok((db_process_child, db_to_monitor_reader, monitor_to_db_writer))
}

fn validate_bysorting(db_in: &mut impl BufRead, expected_output_file_path: &PathBuf) -> Result<()> {
    let mut expected_output_reader = BufReader::new(File::open(expected_output_file_path)?);
    let mut expected_rows = Vec::new();
    loop {
        let mut row = String::new();
        expected_output_reader
            .read_line(&mut row)
            .context("Failed to read row from expected output file")?;
        let trimmed_row = row.trim();
        if trimmed_row.is_empty() {
            break;
        }
        expected_rows.push(trimmed_row.to_string());
    }

    let mut db_output_rows = Vec::new();

    loop {
        let mut db_in_line = String::new();
        db_in
            .read_line(&mut db_in_line)
            .context("Failed to read line from database output")?;

        if db_in_line.trim() == "!" {
            if db_output_rows.len() != expected_rows.len() {
                bail!(
                    "Number of rows didn't match, expected {} but found {}",
                    expected_rows.len(),
                    db_output_rows.len()
                );
            }
            break;
        }
        if db_output_rows.len() == expected_rows.len() {
            bail!(
                "Expected end of row `!`, but db outputed additional row {}",
                db_in_line
            );
        }
        if db_in_line.trim().is_empty() {
            bail!("Empty line found");
        }
        db_output_rows.push(db_in_line.trim().to_string());
    }

    expected_rows.sort();
    db_output_rows.sort();

    for (expected_row, db_row) in expected_rows.iter().zip(db_output_rows.iter()) {
        if !expected_row.eq(db_row) {
            bail!(
                "Expected line output\n{}\nbut database returned\n{}",
                expected_row,
                db_row
            );
        }
    }

    Ok(())
}

fn validate_presorted(db_in: &mut impl BufRead, expected_output_file_path: &PathBuf) -> Result<()> {
    let mut expected_output_reader = BufReader::new(File::open(expected_output_file_path)?);
    let mut line_count = 0;

    loop {
        line_count += 1;
        let mut expected_output_line = String::new();
        let mut db_in_line = String::new();

        expected_output_reader.read_line(&mut expected_output_line)?;
        db_in
            .read_line(&mut db_in_line)
            .context("Failed to read line from database output")?;

        if expected_output_line.trim().is_empty() {
            if db_in_line.trim() != "!" {
                bail!("Expected end of output rows '!'\nbut found\n{}", db_in_line);
            }
            break;
        }

        if expected_output_line.trim() != db_in_line.trim() {
            bail!(
                "Expected line output\n{}\nbut database returned\n{}\nerror at line {line_count}",
                expected_output_line,
                db_in_line
            );
        }
    }

    Ok(())
}

fn handle_db(
    db_in: &mut impl BufRead,
    db_out: &mut impl Write,
    query_config: &QueryConfig,
) -> Result<()> {
    db_out.write_all(format!("{}\n", serde_json::to_string(&query_config.query)?).as_bytes())?;
    db_out.flush()?;

    loop {
        let command = read_command(db_in)?;
        if command.is_empty() {
            break;
        }

        match command[0].to_lowercase().as_ref() {
            "get_memory_limit" => {
                db_out.write_all(format!("{}\n", query_config.memory_limit_mb).as_bytes())?;
            }
            "validate" => {
                match query_config.sort_before_check {
                    true => validate_bysorting(db_in, &query_config.expected_output_file),
                    false => validate_presorted(db_in, &query_config.expected_output_file),
                }?;
                return Ok(());
            }
            other => bail!("Unknown command: {other}"),
        };
    }

    bail!("Program did not validate the result");
}

fn read_command<R: BufRead>(buf_reader: &mut R) -> Result<Vec<String>> {
    loop {
        let mut input_line = String::new();
        let mut limited_reader = buf_reader.take(MAX_COMMAND_LENGTH);
        let read_length = limited_reader
            .read_line(&mut input_line)
            .context("Failed to read input line")?;

        let result: Vec<String> = input_line.split_whitespace().map(String::from).collect();

        if !result.is_empty() || read_length == 0 {
            return Ok(result);
        }
    }
}

fn parse_disk_metrics(stderr_text: &str) -> Result<DiskMetricsRow> {
    let line = stderr_text
        .lines()
        .find(|line| line.starts_with("DISK_IO_METRICS,"))
        .context("disk metrics line not found in disk stderr")?;

    let mut row = DiskMetricsRow::default();

    for field in line.split(',').skip(1) {
        let (key, value) = field
            .split_once('=')
            .with_context(|| format!("invalid disk metrics field: {field}"))?;
        let parsed: u64 = value
            .parse()
            .with_context(|| format!("failed to parse disk metric {key}={value}"))?;
        match key {
            "total_reads" => row.total_reads = parsed,
            "total_writes" => row.total_writes = parsed,
            "total_blocks_processed" => row.total_blocks_processed = parsed,
            "total_cylinders_traveled" => row.total_cylinders_traveled = parsed,
            "total_io_time_us" => row.total_io_time_us = parsed,
            "total_seek_time_us" => row.total_seek_time_us = parsed,
            "total_rotational_latency_us" => row.total_rotational_latency_us = parsed,
            "total_transfer_time_us" => row.total_transfer_time_us = parsed,
            _ => {}
        }
    }

    Ok(row)
}

fn parse_optional_u64(value: &str) -> Result<Option<u64>> {
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.parse()?))
    }
}

fn parse_db_mem_metrics(stderr_text: &str) -> Result<DbMemMetricsRow> {
    let Some(line) = stderr_text
        .lines()
        .find(|line| line.starts_with("DB_MEM_METRICS,"))
    else {
        return Ok(DbMemMetricsRow::default());
    };

    let mut row = DbMemMetricsRow::default();

    for field in line.split(',').skip(1) {
        let (key, value) = field
            .split_once('=')
            .with_context(|| format!("invalid db mem metrics field: {field}"))?;
        let parsed = parse_optional_u64(value)
            .with_context(|| format!("failed to parse db mem metric {key}={value}"))?;

        match key {
            "vm_peak_kb" => row.vm_peak_kb = parsed,
            "vm_size_kb" => row.vm_size_kb = parsed,
            "vm_hwm_kb" => row.vm_hwm_kb = parsed,
            "vm_rss_kb" => row.vm_rss_kb = parsed,
            _ => {}
        }
    }

    Ok(row)
}

fn csv_escape(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn fmt_opt_u64(v: Option<u64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_default()
}

fn fmt_opt_f64(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.3}")).unwrap_or_default()
}

fn create_metrics_csv(path: &Path) -> Result<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;

    writeln!(
        file,
        "execution_name,total_reads,total_writes,total_blocks_processed,total_cylinders_traveled,total_io_time_us,total_seek_time_us,total_rotational_latency_us,total_transfer_time_us,db_vm_peak_kb,db_vm_size_kb,db_vm_hwm_kb,db_vm_rss_kb,db_vm_peak_pct_of_limit"
    )?;

    Ok(file)
}

fn append_metrics_row(
    csv_file: &mut File,
    execution_name: &str,
    metrics: &DiskMetricsRow,
    db_mem: &DbMemMetricsRow,
    memory_limit_mb: u64,
) -> Result<()> {
    let memory_limit_kb = memory_limit_mb * 1024;
    let peak_vm_pct = db_mem
        .vm_peak_kb
        .map(|v| (v as f64) * 100.0 / (memory_limit_kb as f64));

    writeln!(
        csv_file,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        csv_escape(execution_name),
        metrics.total_reads,
        metrics.total_writes,
        metrics.total_blocks_processed,
        metrics.total_cylinders_traveled,
        metrics.total_io_time_us,
        metrics.total_seek_time_us,
        metrics.total_rotational_latency_us,
        metrics.total_transfer_time_us,
        fmt_opt_u64(db_mem.vm_peak_kb),
        fmt_opt_u64(db_mem.vm_size_kb),
        fmt_opt_u64(db_mem.vm_hwm_kb),
        fmt_opt_u64(db_mem.vm_rss_kb),
        fmt_opt_f64(peak_vm_pct),
    )?;
    csv_file.flush()?;
    Ok(())
}

fn monitor_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let monitor_config = MonitorConfig::load_config(cli_options.get_config_path())
        .context("Failed to load monitor config")?;

    let csv_path = PathBuf::from("disk_io_metrics.csv");
    let mut csv_file = create_metrics_csv(&csv_path)?;

    for query_config in monitor_config.get_query_configs() {
        if query_config.disabled {
            continue;
        }

        let (mut disk_process, disk_outbound_reader, disk_inbound_writer) =
            setup_disk_process(&monitor_config)?;

        let (mut db_process, db_outbound_reader, mut db_inbound_writer) = setup_db_process(
            monitor_config.get_database_config(),
            query_config,
            disk_outbound_reader,
            disk_inbound_writer,
        )?;

        let mut db_in = BufReader::new(db_outbound_reader);
        let db_result = handle_db(&mut db_in, &mut db_inbound_writer, query_config);

        if db_result.is_err() {
            let _ = db_process.kill();
        }
        let _ = db_process.wait();

        let mut db_stderr = String::new();
        if let Some(stderr) = db_process.stderr.as_mut() {
            stderr.read_to_string(&mut db_stderr)?;
        }
        let db_mem_metrics = parse_db_mem_metrics(&db_stderr)
            .with_context(|| format!("failed to parse db memory metrics for {}", query_config.execution_name))?;

        let mut disk_stderr = String::new();
        let _disk_status = disk_process.wait()?;
        if let Some(stderr) = disk_process.stderr.as_mut() {
            stderr.read_to_string(&mut disk_stderr)?;
        }

        let disk_metrics = parse_disk_metrics(&disk_stderr)
            .with_context(|| format!("failed to parse disk metrics for {}", query_config.execution_name))?;

        append_metrics_row(
            &mut csv_file,
            &query_config.execution_name,
            &disk_metrics,
            &db_mem_metrics,
            query_config.memory_limit_mb,
        )?;

        println!("--------------------------------------------------------------------------------");
        println!();

        db_result.context(format!(
            "Validation failed! for {}",
            query_config.execution_name
        ))?;

        println!("Validation success! for {}", query_config.execution_name);
    }

    println!("Saved disk IO metrics CSV to disk_io_metrics.csv");
    Ok(())
}

fn main() -> Result<()> {
    monitor_main().with_context(|| "From Monitor")
}
