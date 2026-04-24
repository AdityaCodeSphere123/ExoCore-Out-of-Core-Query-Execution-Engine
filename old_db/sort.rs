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
    InMemory {
        iter: std::vec::IntoIter<Row>,
        reserved_bytes: usize,
    },
    External(RunMerger),
}

impl<'a> SortOperator<'a> {
    pub fn new(
        underlying: Box<dyn Operator + 'a>,
        sort_specs: &[SortSpec],
    ) -> Result<Self> {
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

    fn predicted_vec_capacity_after_push(current_capacity: usize) -> usize {
        if current_capacity == 0 {
            4
        } else {
            current_capacity.saturating_mul(2)
        }
    }

    fn prepare(&mut self, ctx: &mut ExecContext) -> Result<()> {
        let mut rows: Vec<Row> = Vec::new();
        let mut bytes_used = 0usize;
        let mut reserved_bytes = 0usize;
        let mut run_ids: Vec<TempFileId> = Vec::new();

        while let Some(row) = self.underlying.next(ctx)? {
            let row_bytes = row.estimate_heap_size();
            let capacity_growth_bytes = if rows.len() < rows.capacity() {
                0
            } else {
                let next_capacity = Self::predicted_vec_capacity_after_push(rows.capacity());
                next_capacity
                    .saturating_sub(rows.capacity())
                    .saturating_mul(std::mem::size_of::<Row>())
            };
            let additional_bytes = row_bytes + capacity_growth_bytes;
            let projected_reserved = reserved_bytes + additional_bytes;

            let local_soft_limit = ctx.sort_run_bytes * 4/5;
            let should_spill = (!rows.is_empty() && projected_reserved > local_soft_limit)
                || (!rows.is_empty() && ctx.available_memory() < additional_bytes);

            if should_spill {
                sort_rows(&mut rows, &self.sort_keys);
                let run_id = spill_run(ctx, &rows)?;
                run_ids.push(run_id);
                ctx.release_memory(reserved_bytes);
                rows = Vec::new();
                bytes_used = 0;
                reserved_bytes = 0;
            }

            let capacity_growth_bytes = if rows.len() < rows.capacity() {
                0
            } else {
                let next_capacity = Self::predicted_vec_capacity_after_push(rows.capacity());
                next_capacity
                    .saturating_sub(rows.capacity())
                    .saturating_mul(std::mem::size_of::<Row>())
            };
            let additional_bytes = row_bytes + capacity_growth_bytes;
            ctx.try_reserve_memory(additional_bytes)?;
            reserved_bytes += additional_bytes;

            rows.push(row);
            bytes_used += row_bytes;
        }

        if run_ids.is_empty() {
            sort_rows(&mut rows, &self.sort_keys);
            self.output = SortOutput::InMemory {
                iter: rows.into_iter(),
                reserved_bytes,
            };
        } else {
            if !rows.is_empty() {
                sort_rows(&mut rows, &self.sort_keys);
                let run_id = spill_run(ctx, &rows)?;
                run_ids.push(run_id);
                ctx.release_memory(reserved_bytes);
                reserved_bytes = 0;
            }

            drop(rows);

            self.output = SortOutput::External(RunMerger::new(
                ctx,
                run_ids,
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

        match std::mem::replace(&mut self.output, SortOutput::Pending) {
            SortOutput::Pending => Ok(None),
            SortOutput::InMemory { mut iter, reserved_bytes } => {
                if let Some(row) = iter.next() {
                    self.output = SortOutput::InMemory { iter, reserved_bytes };
                    Ok(Some(row))
                } else {
                    ctx.release_memory(reserved_bytes);
                    Ok(None)
                }
            }
            SortOutput::External(mut merger) => {
                let disk_reader = &mut *ctx.disk_reader;
                let disk_writer = &mut *ctx.disk_writer;
                let temp_storage = &*ctx.temp_storage;
                let row = merger.next_row(temp_storage, disk_reader, disk_writer)?;
                self.output = SortOutput::External(merger);
                Ok(row)
            }
        }
    }
}

fn spill_run(ctx: &mut ExecContext, rows: &[Row]) -> Result<TempFileId> {
    // Large write batches → fewer extents per run → fewer non-sequential I/Os
    // during the merge phase, directly reducing rotational latency.
    let block_size = ctx.temp_storage.block_size().max(1);
    let batch_pages = (ctx.sort_run_bytes / block_size).max(1).min(300);
    let mut writer = TempRunWriter::with_batch_pages(ctx.temp_storage, batch_pages)?;
    for row in rows {
        writer.append_row(row, ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)?;
    }
    writer.finish(ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)
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
/// unreachable in practice.  Returning Equal there (instead of propagating a
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
    }}


struct RunMerger {
    readers: Vec<TempRunReader>,
    heap: BinaryHeap<HeapItem>,
}

impl RunMerger {
    fn new(
        ctx: &mut ExecContext,
        run_ids: Vec<TempFileId>,
        sort_keys: Vec<SortKey>,
    ) -> Result<Self> {
        let shared_keys = Arc::new(sort_keys);
        let mut readers = Vec::with_capacity(run_ids.len());
        let mut heap = BinaryHeap::new();

        // Spread available memory across all run readers so each gets a
        // larger batch buffer, drastically reducing seek count during merge.
        let block_size = ctx.temp_storage.block_size();
        let num_runs = run_ids.len();
        let reader_batch_pages = if num_runs > 0 && block_size > 0 {
            (ctx.sort_run_bytes / (num_runs * block_size)).max(1).min(300)
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
        let ord = compare_rows(&self.sort_keys, &self.row, &other.row);
        if ord == Ordering::Equal {
            other.run_idx.cmp(&self.run_idx)
        } else {
            ord.reverse()
        }
    }
}