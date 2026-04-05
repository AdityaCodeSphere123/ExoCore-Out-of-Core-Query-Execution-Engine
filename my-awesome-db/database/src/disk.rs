use anyhow::{anyhow, Result};
use common::{Data, DataType};
use db_config::table::TableSpec;
use db_config::DbContext;
use std::collections::VecDeque;
use std::io::{BufRead, Write};

use crate::buffer::BlockBuffer;
use crate::operator::Operator;
use crate::row::{Row, RowSchema};

const SCAN_PREFETCH_BLOCKS: usize = 16;

pub fn get_table_spec<'a>(ctx: &'a DbContext, table_id: &str) -> Result<&'a TableSpec> {
    ctx.get_table_specs()
        .iter()
        .find(|t| t.name == table_id || t.file_id == table_id)
        .ok_or_else(|| anyhow!("table not found: {}", table_id))
}

pub fn schema_from_table_spec(table_spec: &TableSpec) -> RowSchema {
    RowSchema::new(
        table_spec
            .column_specs
            .iter()
            .map(|c| c.column_name.clone())
            .collect(),
    )
}

pub struct ScanOperator {
    table_id: String,
    schema: RowSchema,
    start_block: Option<u64>,
    num_blocks: Option<u64>,
    current_block_offset: u64,
    current_rows: Option<std::vec::IntoIter<Row>>,
    prefetched_rows: VecDeque<std::vec::IntoIter<Row>>,
}

impl ScanOperator {
    pub fn new(table_id: String, schema: RowSchema) -> Self {
        Self {
            table_id,
            schema,
            start_block: None,
            num_blocks: None,
            current_block_offset: 0,
            current_rows: None,
            prefetched_rows: VecDeque::new(),
        }
    }

    fn refill_prefetch_buffer(&mut self, ctx: &mut crate::operator::ExecContext) -> Result<bool> {
        if self.start_block.is_none() {
            let table_spec = get_table_spec(ctx.db_ctx, &self.table_id)?;
            self.start_block = Some(get_file_start_block(
                ctx.disk_reader,
                ctx.disk_writer,
                &table_spec.file_id,
            )?);
            self.num_blocks = Some(get_file_num_blocks(
                ctx.disk_reader,
                ctx.disk_writer,
                &table_spec.file_id,
            )?);
        }

        let start_block = self.start_block.expect("scan start block must be initialized");
        let num_blocks = self.num_blocks.expect("scan num_blocks must be initialized");

        if self.current_block_offset >= num_blocks {
            return Ok(false);
        }

        let table_spec = get_table_spec(ctx.db_ctx, &self.table_id)?;
        let remaining_blocks = (num_blocks - self.current_block_offset) as usize;
        let batch_blocks = remaining_blocks.min(SCAN_PREFETCH_BLOCKS);
        let block_size = ctx.buffer_manager.block_size();
        let batch_start_block = start_block + self.current_block_offset;

        let batch_buf = get_blocks(
            ctx.disk_reader,
            ctx.disk_writer,
            batch_start_block,
            batch_blocks as u64,
            block_size,
        )?;

        for block_idx in 0..batch_blocks {
            let start = block_idx * block_size;
            let end = start + block_size;
            let block_rows = decode_block_into_rows(table_spec, &batch_buf[start..end])?;
            self.prefetched_rows.push_back(block_rows.into_iter());
        }

        self.current_block_offset += batch_blocks as u64;
        Ok(true)
    }
}

impl Operator for ScanOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self, ctx: &mut crate::operator::ExecContext) -> Result<Option<Row>> {
        loop {
            if let Some(rows) = &mut self.current_rows {
                if let Some(row) = rows.next() {
                    return Ok(Some(row));
                }
                self.current_rows = None;
            }

            if let Some(rows) = self.prefetched_rows.pop_front() {
                self.current_rows = Some(rows);
                continue;
            }

            if !self.refill_prefetch_buffer(ctx)? {
                return Ok(None);
            }
        }
    }
}

pub fn get_block_size<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
) -> Result<usize>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    disk_writer.write_all(b"get block-size\n")?;
    disk_writer.flush()?;

    let mut line = String::new();
    disk_reader.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

pub fn get_file_start_block<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    file_id: &str,
) -> Result<u64>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let cmd = format!("get file start-block {}\n", file_id);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;

    let mut line = String::new();
    disk_reader.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

pub fn get_file_num_blocks<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    file_id: &str,
) -> Result<u64>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let cmd = format!("get file num-blocks {}\n", file_id);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;

    let mut line = String::new();
    disk_reader.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

pub fn get_blocks<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    start_block_id: u64,
    num_blocks: u64,
    block_size: usize,
) -> Result<Vec<u8>>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let cmd = format!("get block {} {}\n", start_block_id, num_blocks);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;

    let mut buf = vec![0u8; block_size * (num_blocks as usize)];
    std::io::Read::read_exact(disk_reader, &mut buf)?;
    Ok(buf)
}

pub fn decode_block_into_rows(table_spec: &TableSpec, block: &[u8]) -> Result<Vec<Row>> {
    let buf = BlockBuffer::new(block);
    let row_count = buf.row_count()?;
    let mut offset = 0usize;

    let mut rows = Vec::with_capacity(row_count);

    for _ in 0..row_count {
        let mut values = Vec::with_capacity(table_spec.column_specs.len());

        for col in &table_spec.column_specs {
            let value = match &col.data_type {
                DataType::Int32 => Data::Int32(buf.read_i32(&mut offset)?),
                DataType::Int64 => Data::Int64(buf.read_i64(&mut offset)?),
                DataType::Float32 => Data::Float32(buf.read_f32(&mut offset)?),
                DataType::Float64 => Data::Float64(buf.read_f64(&mut offset)?),
                DataType::String => Data::String(buf.read_cstring(&mut offset)?),
            };
            values.push(value);
        }

        rows.push(Row::new(values));
    }

    Ok(rows)
}
