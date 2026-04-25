use anyhow::Result;
use common::query::SortSpec;
use common::Data;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufRead, Write};
use std::sync::Arc;

use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};
use crate::temp_storage::{TempFileId, TempRunReader, TempRunWriter, TempStorageManager};

const MAX_SORT_SPILL_BATCH_PAGES: usize = 512;
const MAX_SORT_MERGE_READER_PAGES: usize = 128;
const MAX_SORT_MERGE_TOTAL_READER_PAGES: usize = 128;
const MIN_SORT_MERGE_READER_PAGES: usize = 8;
const SORT_MISC_RESERVE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct SortKey {
    pub idx: usize,
    pub ascending: bool,
}

pub struct SortOperator<'a> {
    underlying: Box<dyn Operator + 'a>,
    schema: RowSchema,
    sort_keys: Vec<SortKey>,
    prepared: bool,
    output: SortOutput,
}

enum SortOutput {
    Pending,
    InMemory(std::vec::IntoIter<Row>),
    External(RunMerger),
}

impl<'a> SortOperator<'a> {
    pub fn new(underlying: Box<dyn Operator + 'a>, sort_specs: &[SortSpec]) -> Result<Self> {
        let schema = underlying.schema().clone();
        let mut sort_keys = Vec::with_capacity(sort_specs.len());

        for spec in sort_specs {
            let idx = schema.require_index(&spec.column_name)?;
            sort_keys.push(SortKey {
                idx,
                ascending: spec.ascending,
            });
        }

        Ok(Self {
            underlying,
            schema,
            sort_keys,
            prepared: false,
            output: SortOutput::Pending,
        })
    }

    fn prepare(&mut self, ctx: &mut ExecContext) -> Result<()> {
        let row_budget = sort_row_budget_bytes(ctx);
        let mut rows: Vec<Row> = Vec::new();
        let mut bytes_used = 0usize;
        let mut run_ids: Vec<TempFileId> = Vec::new();

        while let Some(row) = self.underlying.next(ctx)? {
            let next_row_bytes = row.estimate_heap_size();
            let next_capacity = projected_vec_capacity_after_push(rows.len(), rows.capacity());
            let projected_mem = bytes_used
                .saturating_add(next_row_bytes)
                .saturating_add(next_capacity.saturating_mul(std::mem::size_of::<Row>()));

            if !rows.is_empty() && projected_mem >= row_budget {
                sort_rows(&mut rows, &self.sort_keys);
                let run_id = spill_run(ctx, &rows)?;
                run_ids.push(run_id);

                rows = Vec::new();
                bytes_used = 0;
            }

            bytes_used = bytes_used.saturating_add(next_row_bytes);
            rows.push(row);
        }

        if run_ids.is_empty() {
            sort_rows(&mut rows, &self.sort_keys);
            self.output = SortOutput::InMemory(rows.into_iter());
        } else {
            if !rows.is_empty() {
                sort_rows(&mut rows, &self.sort_keys);
                let run_id = spill_run(ctx, &rows)?;
                run_ids.push(run_id);
            }

            drop(rows);

            // If too many initial runs are handed directly to the final merger,
            // each TempRunReader gets a tiny batch. That hurts the disk simulator
            // because reads bounce across many temp files. Grouped merge collapses
            // runs in passes until the final fan-in is small enough for each reader
            // to receive a decent batch size.
            let final_run_ids = collapse_runs_for_grouped_merge(ctx, run_ids, &self.sort_keys)?;
            self.output = SortOutput::External(RunMerger::new(
                ctx,
                final_run_ids,
                self.sort_keys.clone(),
            )?);
        }

        self.prepared = true;
        Ok(())
    }
}

impl<'a> Operator for SortOperator<'a> {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        if !self.prepared {
            self.prepare(ctx)?;
        }

        match &mut self.output {
            SortOutput::Pending => Ok(None),
            SortOutput::InMemory(iter) => Ok(iter.next()),
            SortOutput::External(merger) => {
                let disk_reader = &mut *ctx.disk_reader;
                let disk_writer = &mut *ctx.disk_writer;
                let temp_storage = &*ctx.temp_storage;
                merger.next_row(temp_storage, disk_reader, disk_writer)
            }
        }
    }
}

fn projected_vec_capacity_after_push(len: usize, cap: usize) -> usize {
    if len < cap {
        cap
    } else if cap == 0 {
        4
    } else {
        cap.saturating_mul(2)
    }
}

fn choose_sort_spill_batch_pages(total_budget_bytes: usize, block_size: usize) -> usize {
    let block_size = block_size.max(1);
    let pages = total_budget_bytes / (8 * block_size);
    pages.clamp(4, MAX_SORT_SPILL_BATCH_PAGES)
}

fn sort_writer_reserve_bytes(total_budget_bytes: usize, block_size: usize) -> usize {
    let spill_pages = choose_sort_spill_batch_pages(total_budget_bytes, block_size);
    spill_pages
        .saturating_add(1)
        .saturating_mul(block_size)
        .saturating_add(SORT_MISC_RESERVE_BYTES)
}

fn sort_row_budget_bytes(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    let reserve = sort_writer_reserve_bytes(ctx.sort_run_bytes, block_size);
    ctx.sort_run_bytes.saturating_sub(reserve).max(block_size)
}

fn sort_merge_reader_budget_bytes(total_budget_bytes: usize, block_size: usize) -> usize {
    let max_reader_budget = MAX_SORT_MERGE_TOTAL_READER_PAGES
        .saturating_mul(block_size)
        .max(block_size);
    total_budget_bytes
        .saturating_div(4)
        .clamp(block_size, max_reader_budget)
}

fn choose_sort_merge_fan_in(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    let total_reader_pages = sort_merge_reader_budget_bytes(ctx.sort_run_bytes, block_size)
        .saturating_div(block_size)
        .max(1);

    // Keep at least MIN_SORT_MERGE_READER_PAGES pages per active run whenever
    // possible. With the default 128-page total reader budget and an 8-page
    // minimum, this gives fan-in 16.
    total_reader_pages
        .saturating_div(MIN_SORT_MERGE_READER_PAGES)
        .max(2)
}

fn spill_run(ctx: &mut ExecContext, rows: &[Row]) -> Result<TempFileId> {
    let block_size = ctx.temp_storage.block_size().max(1);
    let batch_pages = choose_sort_spill_batch_pages(ctx.sort_run_bytes, block_size);
    let mut writer = TempRunWriter::with_batch_pages(ctx.temp_storage, batch_pages)?;
    for row in rows {
        writer.append_row(row, ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)?;
    }
    writer.finish(ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)
}

fn collapse_runs_for_grouped_merge(
    ctx: &mut ExecContext,
    run_ids: Vec<TempFileId>,
    sort_keys: &[SortKey],
) -> Result<Vec<TempFileId>> {
    let fan_in = choose_sort_merge_fan_in(ctx);
    if run_ids.len() <= fan_in {
        return Ok(run_ids);
    }

    let mut current = run_ids;

    while current.len() > fan_in {
        let mut next = Vec::with_capacity((current.len() + fan_in - 1) / fan_in);

        for group in current.chunks(fan_in) {
            if group.len() == 1 {
                next.push(group[0]);
            } else {
                let merged = merge_run_group_to_temp(ctx, group, sort_keys)?;
                next.push(merged);
            }
        }

        current = next;
    }

    Ok(current)
}

fn merge_run_group_to_temp(
    ctx: &mut ExecContext,
    run_ids: &[TempFileId],
    sort_keys: &[SortKey],
) -> Result<TempFileId> {
    debug_assert!(!run_ids.is_empty());

    if run_ids.len() == 1 {
        return Ok(run_ids[0]);
    }

    let input_runs = run_ids.to_vec();
    let block_size = ctx.temp_storage.block_size().max(1);
    let writer_batch_pages = choose_sort_spill_batch_pages(ctx.sort_run_bytes, block_size);
    let mut writer = TempRunWriter::with_batch_pages(ctx.temp_storage, writer_batch_pages)?;
    let mut merger = RunMerger::new(ctx, input_runs.clone(), sort_keys.to_vec())?;

    loop {
        let next_row = {
            let disk_reader = &mut *ctx.disk_reader;
            let disk_writer = &mut *ctx.disk_writer;
            let temp_storage = &*ctx.temp_storage;
            merger.next_row(temp_storage, disk_reader, disk_writer)?
        };

        match next_row {
            Some(row) => writer.append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?,
            None => break,
        }
    }

    drop(merger);

    let output_run = writer.finish(
        ctx.temp_storage,
        &mut *ctx.disk_reader,
        &mut *ctx.disk_writer,
    )?;

    for run_id in input_runs {
        ctx.temp_storage.delete_temp_file(run_id)?;
    }

    Ok(output_run)
}

fn sort_rows(rows: &mut [Row], sort_keys: &[SortKey]) {
    rows.sort_unstable_by(|a, b| compare_rows(sort_keys, a, b));
}

fn compare_rows(sort_keys: &[SortKey], a: &Row, b: &Row) -> Ordering {
    // Access the value slices once outside the loop to avoid repeated method
    // dispatch, and index directly (panics on OOB — indices are validated at
    // SortOperator construction time so this cannot happen).
    let a_vals = a.values();
    let b_vals = b.values();
    for key in sort_keys {
        let ord = compare_data_ord(&a_vals[key.idx], &b_vals[key.idx]);
        let ord = if key.ascending { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Infallible comparison used in the sort hot-path.
///
/// Schema validation at operator construction time guarantees that both sides
/// have the same type for every sort key, so the cross-type arm (`_ =>`) is
/// unreachable in practice. Returning Equal there (instead of propagating a
/// Result) eliminates all error-machinery overhead from the comparison closure
/// called O(N log N) times during a sort.
#[inline]
fn compare_data_ord(left: &Data, right: &Data) -> Ordering {
    match (left, right) {
        (Data::Int32(a), Data::Int32(b)) => a.cmp(b),
        (Data::Int64(a), Data::Int64(b)) => a.cmp(b),
        (Data::Float32(a), Data::Float32(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Data::Float64(a), Data::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Data::String(a), Data::String(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

struct RunMerger {
    readers: Vec<TempRunReader>,
    heap: BinaryHeap<HeapItem>,
}

impl RunMerger {
    fn new(ctx: &mut ExecContext, run_ids: Vec<TempFileId>, sort_keys: Vec<SortKey>) -> Result<Self> {
        let shared_keys = Arc::new(sort_keys);
        let mut readers = Vec::with_capacity(run_ids.len());
        let mut heap = BinaryHeap::new();

        let block_size = ctx.temp_storage.block_size().max(1);
        let num_runs = run_ids.len();
        let total_reader_budget = sort_merge_reader_budget_bytes(ctx.sort_run_bytes, block_size);
        let reader_batch_pages = if num_runs > 0 {
            (total_reader_budget / (num_runs * block_size))
                .max(1)
                .min(MAX_SORT_MERGE_READER_PAGES)
        } else {
            16
        };

        let disk_reader = &mut *ctx.disk_reader;
        let disk_writer = &mut *ctx.disk_writer;
        let temp_storage = &*ctx.temp_storage;

        for (run_idx, run_id) in run_ids.into_iter().enumerate() {
            let mut reader =
                TempRunReader::with_batch_pages(temp_storage, run_id, reader_batch_pages)?;
            if let Some(row) = reader.next_row(temp_storage, disk_reader, disk_writer)? {
                heap.push(HeapItem {
                    row,
                    run_idx,
                    sort_keys: Arc::clone(&shared_keys),
                });
            }
            readers.push(reader);
        }

        Ok(Self { readers, heap })
    }

    fn next_row<RDisk, WDisk>(
        &mut self,
        storage: &TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<Option<Row>>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        let item = match self.heap.pop() {
            Some(item) => item,
            None => return Ok(None),
        };

        let run_idx = item.run_idx;
        let sort_keys = Arc::clone(&item.sort_keys);
        let out_row = item.row;

        if let Some(next_row) = self.readers[run_idx].next_row(storage, disk_reader, disk_writer)? {
            self.heap.push(HeapItem {
                row: next_row,
                run_idx,
                sort_keys,
            });
        }

        Ok(Some(out_row))
    }
}

struct HeapItem {
    row: Row,
    run_idx: usize,
    sort_keys: Arc<Vec<SortKey>>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.run_idx == other.run_idx
            && compare_rows(&self.sort_keys, &self.row, &other.row) == Ordering::Equal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_rows(&self.sort_keys, &other.row, &self.row)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
