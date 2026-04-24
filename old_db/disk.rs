use anyhow::{anyhow, Result};
use common::{Data, DataType};
use db_config::table::TableSpec;
use db_config::DbContext;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::hash::Hasher;
use std::mem::size_of;
use std::io::{BufRead, Write};

use crate::buffer::BlockBuffer;
use crate::filter::{eval_resolved, resolve_predicates, ResolvedPredicate};
use crate::operator::Operator;
use crate::row::{Row, RowSchema};

const SCAN_PREFETCH_BLOCKS: usize = 32;

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


#[derive(Debug, Clone)]
pub enum RuntimeMembershipFilterKind {
    ExactIntBitmap {
        base: i64,
        bits: Vec<u64>,
    },
    Bloom {
        words: Vec<u64>,
        num_hashes: usize,
    },
}

#[derive(Debug, Clone)]
pub struct RuntimeMembershipFilter {
    pub full_col_idx: usize,
    pub kind: RuntimeMembershipFilterKind,
}

impl RuntimeMembershipFilter {
    pub fn exact_int_bitmap(full_col_idx: usize, base: i64, bits: Vec<u64>) -> Self {
        Self {
            full_col_idx,
            kind: RuntimeMembershipFilterKind::ExactIntBitmap { base, bits },
        }
    }

    pub fn bloom(full_col_idx: usize, words: Vec<u64>, num_hashes: usize) -> Result<Self> {
        if words.is_empty() {
            return Err(anyhow!("runtime bloom filter must have at least one word"));
        }
        if num_hashes == 0 {
            return Err(anyhow!("runtime bloom filter must use at least one hash"));
        }
        Ok(Self {
            full_col_idx,
            kind: RuntimeMembershipFilterKind::Bloom { words, num_hashes },
        })
    }

    pub fn matches_data(&self, value: &Data) -> bool {
        match &self.kind {
            RuntimeMembershipFilterKind::ExactIntBitmap { base, bits } => {
                match value {
                    Data::Int32(v) => bitmap_contains(*base, bits, *v as i64),
                    Data::Int64(v) => bitmap_contains(*base, bits, *v),
                    _ => false,
                }
            }
            RuntimeMembershipFilterKind::Bloom { words, num_hashes } => {
                bloom_might_contain(words, *num_hashes, hash_data_value(value))
            }
        }
    }
}

#[derive(Default)]
struct FastHasher {
    state: u64,
}

impl Hasher for FastHasher {
    fn write(&mut self, bytes: &[u8]) {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = if self.state == 0 { FNV_OFFSET } else { self.state };
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        self.state = hash;
    }

    fn write_u8(&mut self, i: u8) { self.write(&[i]); }
    fn write_u16(&mut self, i: u16) { self.write(&i.to_le_bytes()); }
    fn write_u32(&mut self, i: u32) { self.write(&i.to_le_bytes()); }
    fn write_u64(&mut self, i: u64) { self.write(&i.to_le_bytes()); }
    fn write_usize(&mut self, i: usize) { self.write(&i.to_le_bytes()); }
    fn write_i32(&mut self, i: i32) { self.write(&i.to_le_bytes()); }
    fn write_i64(&mut self, i: i64) { self.write(&i.to_le_bytes()); }
    fn finish(&self) -> u64 { self.state }
}

fn bitmap_contains(base: i64, bits: &[u64], value: i64) -> bool {
    if value < base {
        return false;
    }
    let offset = (value - base) as usize;
    let bit_cap = bits.len().saturating_mul(64);
    if offset >= bit_cap {
        return false;
    }
    (bits[offset >> 6] & (1u64 << (offset & 63))) != 0
}

fn bloom_might_contain(words: &[u64], num_hashes: usize, hash: u64) -> bool {
    let total_bits = words.len().saturating_mul(64);
    if total_bits == 0 {
        return false;
    }
    let mut x = hash;
    for _ in 0..num_hashes {
        let bit = (x as usize) % total_bits;
        if (words[bit >> 6] & (1u64 << (bit & 63))) == 0 {
            return false;
        }
        x = x.rotate_left(17).wrapping_mul(0x9e3779b97f4a7c15);
    }
    true
}

fn hash_data_value(value: &Data) -> u64 {
    let mut h = FastHasher::default();
    match value {
        Data::Int32(v) => h.write_i32(*v),
        Data::Int64(v) => h.write_i64(*v),
        Data::Float32(v) => h.write_u32(v.to_bits()),
        Data::Float64(v) => h.write_u64(v.to_bits()),
        Data::String(s) => h.write(s.as_bytes()),
    }
    h.finish()
}

struct BufferedRowBatch {
    rows: std::vec::IntoIter<Row>,
    reserved_bytes: usize,
}

pub struct ScanOperator {
    table_id: String,
    schema: RowSchema,
    /// Per-column keep flag for late materialization.  Empty = keep all columns.
    keep_columns: Vec<bool>,
    /// Local single-relation predicates fused into the scan itself.  These are
    /// resolved against the scan's output schema after column pruning, so rows
    /// that fail never enter the higher operator pipeline.
    scan_filter: Vec<ResolvedPredicate>,
    ordered_bounds: Option<OrderedScanBounds>,
    runtime_membership_filter: Option<RuntimeMembershipFilter>,
    start_block: Option<u64>,
    num_blocks: Option<u64>,
    current_block_offset: u64,
    current_rows: Option<BufferedRowBatch>,
    prefetched_rows: VecDeque<BufferedRowBatch>,
}

impl ScanOperator {
    pub fn new(table_id: String, schema: RowSchema) -> Self {
        Self {
            table_id,
            schema,
            keep_columns: Vec::new(),
            scan_filter: Vec::new(),
            ordered_bounds: None,
            runtime_membership_filter: None,
            start_block: None,
            num_blocks: None,
            current_block_offset: 0,
            current_rows: None,
            prefetched_rows: VecDeque::new(),
        }
    }

    pub fn with_needed_columns(mut self, needed: Vec<usize>, total_columns: usize) -> Self {
        let mut keep = vec![false; total_columns];
        for i in needed {
            keep[i] = true;
        }
        self.keep_columns = keep;
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

    pub fn with_runtime_membership_filter(mut self, filter: RuntimeMembershipFilter) -> Self {
        self.runtime_membership_filter = Some(filter);
        self
    }

    fn estimate_buffered_batch_bytes(rows: &[Row]) -> usize {
        rows.iter().map(Row::estimate_heap_size).sum::<usize>()
            + rows.len().saturating_mul(size_of::<Row>())
    }

    fn refill_prefetch_buffer(&mut self, ctx: &mut crate::operator::ExecContext) -> Result<bool> {
        let table_spec = get_table_spec(ctx.db_ctx, &self.table_id)?;
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
        let block_size = ctx.buffer_manager.block_size();
        let memory_limited_blocks = (ctx.available_memory() / (block_size.max(1) * 2)).max(1);
        let batch_blocks = remaining_blocks.min(SCAN_PREFETCH_BLOCKS).min(memory_limited_blocks);
        let batch_start_block = start_block + self.current_block_offset;

        let batch_buf = get_blocks(
            ctx.disk_reader,
            ctx.disk_writer,
            batch_start_block,
            batch_blocks as u64,
            block_size,
        )?;

        let keep = if self.keep_columns.is_empty() {
            None
        } else {
            Some(self.keep_columns.as_slice())
        };

        for block_idx in 0..batch_blocks {
            let start = block_idx * block_size;
            let end = start + block_size;
            let block_rows = match (&self.runtime_membership_filter, self.scan_filter.is_empty()) {
                (Some(runtime_filter), true) => decode_block_into_rows_runtime_filtered(
                    table_spec,
                    &batch_buf[start..end],
                    keep,
                    runtime_filter,
                    None,
                )?,
                (Some(runtime_filter), false) => decode_block_into_rows_runtime_filtered(
                    table_spec,
                    &batch_buf[start..end],
                    keep,
                    runtime_filter,
                    Some(&self.scan_filter),
                )?,
                (None, true) => decode_block_into_rows(table_spec, &batch_buf[start..end], keep)?,
                (None, false) => decode_block_into_rows_filtered(
                    table_spec,
                    &batch_buf[start..end],
                    keep,
                    &self.scan_filter,
                )?,
            };
            let reserved_bytes = Self::estimate_buffered_batch_bytes(&block_rows);
            ctx.try_reserve_memory(reserved_bytes)?;
            self.prefetched_rows.push_back(BufferedRowBatch {
                rows: block_rows.into_iter(),
                reserved_bytes,
            });
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
            if let Some(batch) = &mut self.current_rows {
                if let Some(row) = batch.rows.next() {
                    return Ok(Some(row));
                }
                let released = batch.reserved_bytes;
                self.current_rows = None;
                ctx.release_memory(released);
            }

            if let Some(batch) = self.prefetched_rows.pop_front() {
                self.current_rows = Some(batch);
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

pub fn decode_block_into_rows(
    table_spec: &TableSpec,
    block: &[u8],
    keep: Option<&[bool]>,
) -> Result<Vec<Row>> {
    decode_block_into_rows_maybe_filtered(table_spec, block, keep, None, None)
}

pub fn decode_block_into_rows_filtered(
    table_spec: &TableSpec,
    block: &[u8],
    keep: Option<&[bool]>,
    scan_filter: &[ResolvedPredicate],
) -> Result<Vec<Row>> {
    decode_block_into_rows_maybe_filtered(table_spec, block, keep, Some(scan_filter), None)
}

pub fn decode_block_into_rows_runtime_filtered(
    table_spec: &TableSpec,
    block: &[u8],
    keep: Option<&[bool]>,
    runtime_filter: &RuntimeMembershipFilter,
    scan_filter: Option<&[ResolvedPredicate]>,
) -> Result<Vec<Row>> {
    decode_block_into_rows_maybe_filtered(
        table_spec,
        block,
        keep,
        scan_filter,
        Some(runtime_filter),
    )
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

fn decode_block_into_rows_maybe_filtered(
    table_spec: &TableSpec,
    block: &[u8],
    keep: Option<&[bool]>,
    scan_filter: Option<&[ResolvedPredicate]>,
    runtime_filter: Option<&RuntimeMembershipFilter>,
) -> Result<Vec<Row>> {
    if let Some(runtime_filter) = runtime_filter {
        return decode_block_into_rows_with_runtime_filter(
            table_spec,
            block,
            keep,
            scan_filter,
            runtime_filter,
        );
    }

    let buf = BlockBuffer::new(block);
    let row_count = buf.row_count()?;
    let mut offset = 0usize;

    let num_kept = keep
        .map(|k| k.iter().filter(|&&v| v).count())
        .unwrap_or(table_spec.column_specs.len());
    let mut rows = Vec::with_capacity(row_count);

    // Build the decode plan once outside the row loop.
    let plan = build_col_decode_plan(table_spec, keep);

    for _ in 0..row_count {
        let mut values = Vec::with_capacity(num_kept);

        for &op in &plan {
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

fn decode_block_into_rows_with_runtime_filter(
    table_spec: &TableSpec,
    block: &[u8],
    keep: Option<&[bool]>,
    scan_filter: Option<&[ResolvedPredicate]>,
    runtime_filter: &RuntimeMembershipFilter,
) -> Result<Vec<Row>> {
    let buf = BlockBuffer::new(block);
    let row_count = buf.row_count()?;
    let mut offset = 0usize;

    let num_kept = keep
        .map(|k| k.iter().filter(|&&v| v).count())
        .unwrap_or(table_spec.column_specs.len());
    let mut rows = Vec::with_capacity(row_count);

    for _ in 0..row_count {
        let mut values = Vec::with_capacity(num_kept);
        let mut row_matches_runtime = true;

        for (col_idx, col) in table_spec.column_specs.iter().enumerate() {
            let needed = keep.map(|k| k[col_idx]).unwrap_or(true);
            let is_runtime_col = col_idx == runtime_filter.full_col_idx;

            if is_runtime_col {
                if needed {
                    let val = read_data_value(&buf, &mut offset, &col.data_type)?;
                    row_matches_runtime = runtime_filter.matches_data(&val);
                    if row_matches_runtime {
                        values.push(val);
                    }
                } else {
                    row_matches_runtime = matches_runtime_filter_encoded(
                        runtime_filter,
                        &buf,
                        &mut offset,
                        &col.data_type,
                    )?;
                }

                if !row_matches_runtime {
                    skip_remaining_columns(table_spec, &buf, &mut offset, col_idx + 1)?;
                    break;
                }
                continue;
            }

            if needed {
                values.push(read_data_value(&buf, &mut offset, &col.data_type)?);
            } else {
                skip_data_value(&buf, &mut offset, &col.data_type)?;
            }
        }

        if !row_matches_runtime {
            continue;
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

fn matches_runtime_filter_encoded(
    runtime_filter: &RuntimeMembershipFilter,
    buf: &BlockBuffer<'_>,
    offset: &mut usize,
    data_type: &DataType,
) -> Result<bool> {
    Ok(match (&runtime_filter.kind, data_type) {
        (RuntimeMembershipFilterKind::ExactIntBitmap { base, bits }, DataType::Int32) => {
            bitmap_contains(*base, bits, buf.read_i32(offset)? as i64)
        }
        (RuntimeMembershipFilterKind::ExactIntBitmap { base, bits }, DataType::Int64) => {
            bitmap_contains(*base, bits, buf.read_i64(offset)?)
        }
        (RuntimeMembershipFilterKind::ExactIntBitmap { .. }, DataType::Float32) => {
            let _ = buf.read_f32(offset)?;
            false
        }
        (RuntimeMembershipFilterKind::ExactIntBitmap { .. }, DataType::Float64) => {
            let _ = buf.read_f64(offset)?;
            false
        }
        (RuntimeMembershipFilterKind::ExactIntBitmap { .. }, DataType::String) => {
            buf.skip_cstring(offset)?;
            false
        }
        (RuntimeMembershipFilterKind::Bloom { words, num_hashes }, DataType::Int32) => {
            let mut h = FastHasher::default();
            h.write_i32(buf.read_i32(offset)?);
            bloom_might_contain(words, *num_hashes, h.finish())
        }
        (RuntimeMembershipFilterKind::Bloom { words, num_hashes }, DataType::Int64) => {
            let mut h = FastHasher::default();
            h.write_i64(buf.read_i64(offset)?);
            bloom_might_contain(words, *num_hashes, h.finish())
        }
        (RuntimeMembershipFilterKind::Bloom { words, num_hashes }, DataType::Float32) => {
            let mut h = FastHasher::default();
            h.write_u32(buf.read_f32(offset)?.to_bits());
            bloom_might_contain(words, *num_hashes, h.finish())
        }
        (RuntimeMembershipFilterKind::Bloom { words, num_hashes }, DataType::Float64) => {
            let mut h = FastHasher::default();
            h.write_u64(buf.read_f64(offset)?.to_bits());
            bloom_might_contain(words, *num_hashes, h.finish())
        }
        (RuntimeMembershipFilterKind::Bloom { words, num_hashes }, DataType::String) => {
            let hash = hash_cstring_in_block(buf, offset)?;
            bloom_might_contain(words, *num_hashes, hash)
        }
    })
}

fn hash_cstring_in_block(buf: &BlockBuffer<'_>, offset: &mut usize) -> Result<u64> {
    let usable_end = buf.usable_end()?;
    let bytes = buf.as_slice();
    let start = *offset;
    while *offset < usable_end && bytes[*offset] != 0 {
        *offset += 1;
    }
    if *offset >= usable_end {
        return Err(anyhow!("unterminated string while hashing runtime membership key"));
    }
    let mut h = FastHasher::default();
    h.write(&bytes[start..*offset]);
    *offset += 1;
    Ok(h.finish())
}

fn skip_remaining_columns(
    table_spec: &TableSpec,
    buf: &BlockBuffer<'_>,
    offset: &mut usize,
    start_col_idx: usize,
) -> Result<()> {
    for col in table_spec.column_specs.iter().skip(start_col_idx) {
        skip_data_value(buf, offset, &col.data_type)?;
    }
    Ok(())
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