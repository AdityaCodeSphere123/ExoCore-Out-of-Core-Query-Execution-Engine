use anyhow::{anyhow, Result};
use common::{Data, DataType};
use db_config::table::TableSpec;
use db_config::DbContext;
use std::io::{BufRead, Write};

use crate::buffer::BlockBuffer;
use crate::buffer_manager::BufferManager;
use crate::row::{Row, RowSchema};

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

use crate::operator::{ExecContext, Operator};

pub fn scan_table<RDisk, WDisk>(
    ctx: &DbContext,
    table_id: &str,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    buffer_manager: &mut BufferManager,
) -> Result<(RowSchema, Vec<Row>)>
where
    RDisk: BufRead,
    WDisk: Write,
{
    let table_spec = get_table_spec(ctx, table_id)?;
    let schema = schema_from_table_spec(table_spec);
    let mut scanner = ScanOperator::new(table_id.to_string(), schema.clone());
    let mut ctx = ExecContext {
        db_ctx: ctx,
        disk_reader,
        disk_writer,
        buffer_manager,
    };

    let mut rows = Vec::new();
    while let Some(row) = scanner.next(&mut ctx)? {
        rows.push(row);
    }
    Ok((schema, rows))
}

pub struct ScanOperator {
    table_id: String,
    schema: RowSchema,
    start_block: Option<u64>,
    num_blocks: Option<u64>,
    current_block_offset: u64,
    current_rows: Option<std::vec::IntoIter<Row>>,
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
        }
    }
}

impl Operator for ScanOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        loop {
            if let Some(rows) = &mut self.current_rows {
                if let Some(row) = rows.next() {
                    return Ok(Some(row));
                }
                self.current_rows = None;
            }

            // Need to load next block
            if self.start_block.is_none() {
                let table_spec = get_table_spec(ctx.db_ctx, &self.table_id)?;
                self.start_block = Some(get_file_start_block(ctx.disk_reader, ctx.disk_writer, &table_spec.file_id)?);
                self.num_blocks = Some(get_file_num_blocks(ctx.disk_reader, ctx.disk_writer, &table_spec.file_id)?);
            }

            let start_block = self.start_block.unwrap();
            let num_blocks = self.num_blocks.unwrap();

            if self.current_block_offset >= num_blocks {
                return Ok(None);
            }

            let block_id = start_block + self.current_block_offset;
            let block = ctx.buffer_manager.get_block(block_id, ctx.disk_reader, ctx.disk_writer)?;
            let table_spec = get_table_spec(ctx.db_ctx, &self.table_id)?;
            let block_rows = decode_block_into_rows(table_spec, block)?;
            
            self.current_rows = Some(block_rows.into_iter());
            self.current_block_offset += 1;
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