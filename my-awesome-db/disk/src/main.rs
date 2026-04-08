use std::{
    io::{stdin, stdout},
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use disk_config::DiskSimulationConfig;

use crate::{cli::CliOptions, disk_simulation::DiskSimulator};

mod cli;
mod disk_simulation;

fn disk_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let disk_simulation_config =
        DiskSimulationConfig::load_disk_simulation_config(cli_options.get_config_path())
            .context("Failed to load disk simulation config")?;

    let mut disk_simulator =
        DiskSimulator::new(disk_simulation_config, stdin().lock(), stdout().lock());

    let disk_io_metrics = disk_simulator.simulate()?;
    // Sleep some time expecting any output of db process would get flushed first
    std::thread::sleep(Duration::from_millis(100));

    eprintln!(
        "DISK_IO_METRICS,total_reads={},total_writes={},total_blocks_processed={},total_cylinders_traveled={},total_io_time_us={},total_seek_time_us={},total_rotational_latency_us={},total_transfer_time_us={}",
        disk_io_metrics.total_reads,
        disk_io_metrics.total_writes,
        disk_io_metrics.total_blocks_processed,
        disk_io_metrics.total_cylinders_traveled,
        disk_io_metrics.total_io_time_us,
        disk_io_metrics.total_seek_time_us,
        disk_io_metrics.total_rotational_latency_us,
        disk_io_metrics.total_transfer_time_us,
    );

    Ok(())
}

fn main() -> Result<()> {
    disk_main().with_context(|| "From Disk")
}
