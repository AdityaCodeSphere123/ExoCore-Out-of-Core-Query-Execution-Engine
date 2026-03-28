use anyhow::{anyhow, Result};
use common::DataType;
use db_config::table::TableSpec;
use db_config::DbContext;
use std::io::{BufRead, Write};

use crate::buffer::BlockBuffer;
use crate::buffer_manager::BufferManager;

pub fn scan_table<RDisk, WDisk, WMon>(
    ctx: &DbContext,
    table_id: &str,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    monitor_writer: &mut WMon,
) -> Result<()>
where
    RDisk: BufRead,
    WDisk: Write,
    WMon: Write,
{
    let table_spec = ctx
        .get_table_specs()
        .iter()
        .find(|t| t.name == table_id || t.file_id == table_id)
        .ok_or_else(|| anyhow!("table not found: {}", table_id))?;

    let file_id = &table_spec.file_id;

    let block_size = get_block_size(disk_reader, disk_writer)?;
    let start_block = get_file_start_block(disk_reader, disk_writer, file_id)?;
    let num_blocks = get_file_num_blocks(disk_reader, disk_writer, file_id)?;

    eprintln!(
        "scan table={} file={} start_block={} num_blocks={} block_size={}",
        table_spec.name, table_spec.file_id, start_block, num_blocks, block_size
    );

    // Keep this modest for now. For a sequential scan, caching will not help much,
    // but this gives you the right architecture.
    let mut buffer_manager = BufferManager::new(block_size, 8)?;

    monitor_writer.write_all(b"validate\n")?;

    for block_offset in 0..num_blocks {
        let block_id = start_block + block_offset;
        let block = buffer_manager.get_block(block_id, disk_reader, disk_writer)?;
        decode_block_and_emit_rows(table_spec, block, monitor_writer)?;
    }

    monitor_writer.write_all(b"!\n")?;
    monitor_writer.flush()?;

    Ok(())
}

fn get_block_size<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
) -> Result<usize>
where
    RDisk: BufRead,
    WDisk: Write,
{
    disk_writer.write_all(b"get block-size\n")?;
    disk_writer.flush()?;

    let mut line = String::new();
    disk_reader.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

fn get_file_start_block<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    file_id: &str,
) -> Result<u64>
where
    RDisk: BufRead,
    WDisk: Write,
{
    let cmd = format!("get file start-block {}\n", file_id);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;

    let mut line = String::new();
    disk_reader.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

fn get_file_num_blocks<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    file_id: &str,
) -> Result<u64>
where
    RDisk: BufRead,
    WDisk: Write,
{
    let cmd = format!("get file num-blocks {}\n", file_id);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;

    let mut line = String::new();
    disk_reader.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

fn decode_block_and_emit_rows<WMon>(
    table_spec: &TableSpec,
    block: &[u8],
    monitor_writer: &mut WMon,
) -> Result<()>
where
    WMon: Write,
{
    let buf = BlockBuffer::new(block);
    let row_count = buf.row_count()?;
    let mut offset = 0usize;

    let mut block_out = String::with_capacity(row_count * 64);

    for _ in 0..row_count {
        for col in &table_spec.column_specs {
            let val = match &col.data_type {
                DataType::Int32 => buf.read_i32(&mut offset)?.to_string(),
                DataType::Int64 => buf.read_i64(&mut offset)?.to_string(),
                DataType::Float32 => buf.read_f32(&mut offset)?.to_string(),
                DataType::Float64 => buf.read_f64(&mut offset)?.to_string(),
                DataType::String => buf.read_cstring(&mut offset)?,
            };
            block_out.push_str(&val);
            block_out.push('|');
        }
        block_out.push('\n');
    }

    monitor_writer.write_all(block_out.as_bytes())?;
    Ok(())
}