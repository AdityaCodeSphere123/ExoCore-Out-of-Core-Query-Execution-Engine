use anyhow::{anyhow, Result};
use common::{Data, DataType};
use db_config::table::TableSpec;
use db_config::DbContext;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io::{BufRead, Write};

use crate::buffer::BlockBuffer;
use crate::filter::{eval_resolved, resolve_predicates, ResolvedPredicate};
use crate::operator::Operator;
use crate::row::{Row, RowSchema};

const SCAN_PREFETCH_BLOCKS: usize = 128;

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

#[derive(Debug, Clone)]
pub struct ScanBound {
    pub value: Data,
    pub inclusive: bool,
}

#[derive(Debug, Clone)]
pub struct OrderedScanBounds {
    pub ordered_col_idx: usize,
    pub lower: Option<ScanBound>,
    pub upper: Option<ScanBound>,
}

pub struct ScanOperator {
    table_id: String,
    schema: RowSchema,
    /// Per-column keep flag for late materialization. Empty = keep all columns.
    keep_columns: Vec<bool>,
    /// Local single-relation predicates fused into the scan itself. These are
    /// resolved against the scan's output schema after column pruning, so rows
    /// that fail never enter the higher operator pipeline.
    scan_filter: Vec<ResolvedPredicate>,
    ordered_bounds: Option<OrderedScanBounds>,
    start_block: Option<u64>,
    num_blocks: Option<u64>,
    current_block_offset: u64,
    current_rows: Option<std::vec::IntoIter<Row>>,
    prefetched_rows: VecDeque<std::vec::IntoIter<Row>>,

    /// Reused read buffer for scan prefetch batches.
    /// Avoids allocating a fresh Vec<u8> on every refill.
    batch_buf: Vec<u8>,

    /// Cached decode plan for this scan's projected columns.
    /// Built once after we know the table spec and keep_columns.
    decode_plan: Option<Vec<ColDecodeOp>>,

    /// Number of output columns produced by decode_plan.
    num_kept: usize,
}

impl ScanOperator {
    pub fn new(table_id: String, schema: RowSchema) -> Self {
        let num_kept = schema.len();

        Self {
            table_id,
            schema,
            keep_columns: Vec::new(),
            scan_filter: Vec::new(),
            ordered_bounds: None,
            start_block: None,
            num_blocks: None,
            current_block_offset: 0,
            current_rows: None,
            prefetched_rows: VecDeque::new(),
            batch_buf: Vec::new(),
            decode_plan: None,
            num_kept,
        }
    }

    pub fn with_needed_columns(mut self, needed: Vec<usize>, total_columns: usize) -> Self {
        let mut keep = vec![false; total_columns];
        for i in needed {
            keep[i] = true;
        }

        self.num_kept = keep.iter().filter(|&&v| v).count();
        self.keep_columns = keep;
        self.decode_plan = None;
        self
    }

    pub fn with_scan_filter_predicates(mut self, predicates: &[common::query::Predicate]) -> Result<Self> {
        self.scan_filter = resolve_predicates(&self.schema, predicates)?;
        Ok(self)
    }

    pub fn with_ordered_bounds(mut self, bounds: OrderedScanBounds) -> Self {
        self.ordered_bounds = Some(bounds);
        self
    }

    fn ensure_decode_plan(&mut self, table_spec: &TableSpec) {
        if self.decode_plan.is_some() {
            return;
        }

        let keep = if self.keep_columns.is_empty() {
            None
        } else {
            Some(self.keep_columns.as_slice())
        };

        self.num_kept = keep
            .map(|k| k.iter().filter(|&&v| v).count())
            .unwrap_or(table_spec.column_specs.len());

        self.decode_plan = Some(build_col_decode_plan(table_spec, keep));
    }

    fn refill_prefetch_buffer(&mut self, ctx: &mut crate::operator::ExecContext) -> Result<bool> {
        let table_spec = get_table_spec(ctx.db_ctx, &self.table_id)?;

        self.ensure_decode_plan(table_spec);

        if self.start_block.is_none() {
            let file_start = get_file_start_block(
                ctx.disk_reader,
                ctx.disk_writer,
                &table_spec.file_id,
            )?;
            let file_blocks = get_file_num_blocks(
                ctx.disk_reader,
                ctx.disk_writer,
                &table_spec.file_id,
            )?;
            let block_size = ctx.buffer_manager.block_size();
            let (scan_start, scan_blocks) = if let Some(bounds) = &self.ordered_bounds {
                restrict_scan_range(
                    table_spec,
                    ctx.disk_reader,
                    ctx.disk_writer,
                    file_start,
                    file_blocks,
                    block_size,
                    bounds,
                )?
            } else {
                (file_start, file_blocks)
            };
            self.start_block = Some(scan_start);
            self.num_blocks = Some(scan_blocks);
        }

        let start_block = self.start_block.expect("scan start block must be initialized");
        let num_blocks = self.num_blocks.expect("scan num_blocks must be initialized");

        if self.current_block_offset >= num_blocks {
            return Ok(false);
        }

        let remaining_blocks = (num_blocks - self.current_block_offset) as usize;
        let batch_blocks = remaining_blocks.min(SCAN_PREFETCH_BLOCKS);
        let block_size = ctx.buffer_manager.block_size();
        let batch_start_block = start_block + self.current_block_offset;

        get_blocks_into(
            ctx.disk_reader,
            ctx.disk_writer,
            batch_start_block,
            batch_blocks as u64,
            block_size,
            &mut self.batch_buf,
        )?;

        let decode_plan = self
            .decode_plan
            .as_ref()
            .expect("decode plan must be initialized before scan refill");

        for block_idx in 0..batch_blocks {
            let start = block_idx * block_size;
            let end = start + block_size;

            let block_rows = decode_block_into_rows_with_plan(
                &self.batch_buf[start..end],
                decode_plan,
                self.num_kept,
                if self.scan_filter.is_empty() {
                    None
                } else {
                    Some(self.scan_filter.as_slice())
                },
            )?;

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
    let mut buf = Vec::new();
    get_blocks_into(
        disk_reader,
        disk_writer,
        start_block_id,
        num_blocks,
        block_size,
        &mut buf,
    )?;
    Ok(buf)
}

pub fn get_blocks_into<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    start_block_id: u64,
    num_blocks: u64,
    block_size: usize,
    out: &mut Vec<u8>,
) -> Result<()>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let cmd = format!("get block {} {}\n", start_block_id, num_blocks);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;

    let needed_len = block_size
        .checked_mul(num_blocks as usize)
        .ok_or_else(|| anyhow!("get_blocks buffer size overflow"))?;

    out.resize(needed_len, 0);
    std::io::Read::read_exact(disk_reader, out)?;
    Ok(())
}

pub fn decode_block_into_rows(
    table_spec: &TableSpec,
    block: &[u8],
    keep: Option<&[bool]>,
) -> Result<Vec<Row>> {
    let num_kept = keep
        .map(|k| k.iter().filter(|&&v| v).count())
        .unwrap_or(table_spec.column_specs.len());

    let plan = build_col_decode_plan(table_spec, keep);
    decode_block_into_rows_with_plan(block, &plan, num_kept, None)
}

pub fn decode_block_into_rows_filtered(
    table_spec: &TableSpec,
    block: &[u8],
    keep: Option<&[bool]>,
    scan_filter: &[ResolvedPredicate],
) -> Result<Vec<Row>> {
    let num_kept = keep
        .map(|k| k.iter().filter(|&&v| v).count())
        .unwrap_or(table_spec.column_specs.len());

    let plan = build_col_decode_plan(table_spec, keep);
    decode_block_into_rows_with_plan(block, &plan, num_kept, Some(scan_filter))
}

fn decode_block_into_rows_with_plan(
    block: &[u8],
    plan: &[ColDecodeOp],
    num_kept: usize,
    scan_filter: Option<&[ResolvedPredicate]>,
) -> Result<Vec<Row>> {
    let buf = BlockBuffer::new(block);
    let row_count = buf.row_count()?;
    let mut offset = 0usize;
    let mut rows = Vec::with_capacity(row_count);

    for _ in 0..row_count {
        let mut values = Vec::with_capacity(num_kept);

        for &op in plan {
            match op {
                ColDecodeOp::KeepI32 => values.push(Data::Int32(buf.read_i32(&mut offset)?)),
                ColDecodeOp::KeepI64 => values.push(Data::Int64(buf.read_i64(&mut offset)?)),
                ColDecodeOp::KeepF32 => values.push(Data::Float32(buf.read_f32(&mut offset)?)),
                ColDecodeOp::KeepF64 => values.push(Data::Float64(buf.read_f64(&mut offset)?)),
                ColDecodeOp::KeepStr => values.push(Data::String(buf.read_cstring(&mut offset)?)),
                ColDecodeOp::SkipFixed(n) => {
                    buf.ensure_bytes(offset, n)?;
                    offset += n;
                }
                ColDecodeOp::SkipStr => buf.skip_cstring(&mut offset)?,
            }
        }

        let row = Row::new(values);
        if let Some(preds) = scan_filter {
            if !eval_resolved(&row, preds)? {
                continue;
            }
        }
        rows.push(row);
    }

    Ok(rows)
}

/// A pre-compiled per-column decode action.  Built once before the row loop so
/// the inner loop only dispatches on this small enum, avoiding the per-row
/// `keep` slice lookup and `DataType` match on every column of every row.
/// Consecutive skipped fixed-width columns are merged into a single `SkipFixed`
/// entry so only one bounds check is needed for the whole run.
#[derive(Clone, Copy)]
enum ColDecodeOp {
    KeepI32,
    KeepI64,
    KeepF32,
    KeepF64,
    KeepStr,
    SkipFixed(usize), // skip N bytes in one bounds check
    SkipStr,
}

fn build_col_decode_plan(table_spec: &TableSpec, keep: Option<&[bool]>) -> Vec<ColDecodeOp> {
    let mut plan: Vec<ColDecodeOp> = Vec::with_capacity(table_spec.column_specs.len());
    let mut pending_skip_bytes = 0usize;

    for (col_idx, col) in table_spec.column_specs.iter().enumerate() {
        let needed = keep.map(|k| k[col_idx]).unwrap_or(true);
        if needed {
            if pending_skip_bytes > 0 {
                plan.push(ColDecodeOp::SkipFixed(pending_skip_bytes));
                pending_skip_bytes = 0;
            }
            plan.push(match &col.data_type {
                DataType::Int32 => ColDecodeOp::KeepI32,
                DataType::Int64 => ColDecodeOp::KeepI64,
                DataType::Float32 => ColDecodeOp::KeepF32,
                DataType::Float64 => ColDecodeOp::KeepF64,
                DataType::String => ColDecodeOp::KeepStr,
            });
        } else {
            match &col.data_type {
                DataType::Int32 => pending_skip_bytes += 4,
                DataType::Int64 => pending_skip_bytes += 8,
                DataType::Float32 => pending_skip_bytes += 4,
                DataType::Float64 => pending_skip_bytes += 8,
                DataType::String => {
                    if pending_skip_bytes > 0 {
                        plan.push(ColDecodeOp::SkipFixed(pending_skip_bytes));
                        pending_skip_bytes = 0;
                    }
                    plan.push(ColDecodeOp::SkipStr);
                }
            }
        }
    }
    if pending_skip_bytes > 0 {
        plan.push(ColDecodeOp::SkipFixed(pending_skip_bytes));
    }
    plan
}


fn restrict_scan_range<RDisk, WDisk>(
    table_spec: &TableSpec,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    file_start_block: u64,
    file_num_blocks: u64,
    block_size: usize,
    bounds: &OrderedScanBounds,
) -> Result<(u64, u64)>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    if file_num_blocks == 0 {
        return Ok((file_start_block, 0));
    }

    let start_idx = if let Some(lower) = &bounds.lower {
        first_block_matching_lower(
            table_spec,
            disk_reader,
            disk_writer,
            file_start_block,
            file_num_blocks,
            block_size,
            bounds.ordered_col_idx,
            lower,
        )?
    } else {
        0
    };

    let end_idx = if let Some(upper) = &bounds.upper {
        first_block_past_upper(
            table_spec,
            disk_reader,
            disk_writer,
            file_start_block,
            file_num_blocks,
            block_size,
            bounds.ordered_col_idx,
            upper,
        )?
    } else {
        file_num_blocks
    };

    if start_idx >= end_idx {
        Ok((file_start_block, 0))
    } else {
        Ok((file_start_block + start_idx, end_idx - start_idx))
    }
}

fn first_block_matching_lower<RDisk, WDisk>(
    table_spec: &TableSpec,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    file_start_block: u64,
    file_num_blocks: u64,
    block_size: usize,
    ordered_col_idx: usize,
    lower: &ScanBound,
) -> Result<u64>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let mut lo = 0u64;
    let mut hi = file_num_blocks;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (_, block_max) = ordered_column_min_max_in_block(
            table_spec,
            disk_reader,
            disk_writer,
            file_start_block + mid,
            block_size,
            ordered_col_idx,
        )?;
        if is_strictly_before_bound(&block_max, lower)? {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn first_block_past_upper<RDisk, WDisk>(
    table_spec: &TableSpec,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    file_start_block: u64,
    file_num_blocks: u64,
    block_size: usize,
    ordered_col_idx: usize,
    upper: &ScanBound,
) -> Result<u64>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let mut lo = 0u64;
    let mut hi = file_num_blocks;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (block_min, _) = ordered_column_min_max_in_block(
            table_spec,
            disk_reader,
            disk_writer,
            file_start_block + mid,
            block_size,
            ordered_col_idx,
        )?;
        if is_strictly_after_bound(&block_min, upper)? {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    Ok(lo)
}

fn ordered_column_min_max_in_block<RDisk, WDisk>(
    table_spec: &TableSpec,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    block_id: u64,
    block_size: usize,
    ordered_col_idx: usize,
) -> Result<(Data, Data)>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let block = get_blocks(disk_reader, disk_writer, block_id, 1, block_size)?;
    let buf = BlockBuffer::new(&block);
    let row_count = buf.row_count()?;
    let mut offset = 0usize;
    let mut first: Option<Data> = None;
    let mut last: Option<Data> = None;

    for _ in 0..row_count {
        for (col_idx, col) in table_spec.column_specs.iter().enumerate() {
            if col_idx == ordered_col_idx {
                let value = read_data_value(&buf, &mut offset, &col.data_type)?;
                if first.is_none() {
                    first = Some(value.clone());
                }
                last = Some(value);
            } else {
                skip_data_value(&buf, &mut offset, &col.data_type)?;
            }
        }
    }

    let first = first.ok_or_else(|| anyhow!("ordered scan hit empty block {}", block_id))?;
    let last = last.ok_or_else(|| anyhow!("ordered scan hit empty block {}", block_id))?;
    Ok((first, last))
}

fn read_data_value(buf: &BlockBuffer<'_>, offset: &mut usize, data_type: &DataType) -> Result<Data> {
    Ok(match data_type {
        DataType::Int32 => Data::Int32(buf.read_i32(offset)?),
        DataType::Int64 => Data::Int64(buf.read_i64(offset)?),
        DataType::Float32 => Data::Float32(buf.read_f32(offset)?),
        DataType::Float64 => Data::Float64(buf.read_f64(offset)?),
        DataType::String => Data::String(buf.read_cstring(offset)?),
    })
}

fn skip_data_value(buf: &BlockBuffer<'_>, offset: &mut usize, data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Int32 => {
            buf.ensure_bytes(*offset, 4)?;
            *offset += 4;
        }
        DataType::Int64 => {
            buf.ensure_bytes(*offset, 8)?;
            *offset += 8;
        }
        DataType::Float32 => {
            buf.ensure_bytes(*offset, 4)?;
            *offset += 4;
        }
        DataType::Float64 => {
            buf.ensure_bytes(*offset, 8)?;
            *offset += 8;
        }
        DataType::String => buf.skip_cstring(offset)?,
    }
    Ok(())
}

fn compare_data_values(left: &Data, right: &Data) -> Result<Ordering> {
    left.partial_cmp(right)
        .ok_or_else(|| anyhow!("cannot compare ordered scan bound with block value"))
}

fn is_strictly_before_bound(value: &Data, bound: &ScanBound) -> Result<bool> {
    let ord = compare_data_values(value, &bound.value)?;
    Ok(match ord {
        Ordering::Less => true,
        Ordering::Equal => !bound.inclusive,
        Ordering::Greater => false,
    })
}

fn is_strictly_after_bound(value: &Data, bound: &ScanBound) -> Result<bool> {
    let ord = compare_data_values(value, &bound.value)?;
    Ok(match ord {
        Ordering::Greater => true,
        Ordering::Equal => !bound.inclusive,
        Ordering::Less => false,
    })
}
