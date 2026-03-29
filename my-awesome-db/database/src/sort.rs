use anyhow::{anyhow, Result};
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
    InMemory(std::vec::IntoIter<Row>),
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

    fn prepare(&mut self, ctx: &mut ExecContext) -> Result<()> {
        let mut rows: Vec<Row> = Vec::new();
        let mut bytes_used = 0usize;
        let mut run_ids: Vec<TempFileId> = Vec::new();

        while let Some(row) = self.underlying.next(ctx)? {
            bytes_used += estimate_row_size(&row);
            rows.push(row);

            if bytes_used >= ctx.sort_run_bytes && !rows.is_empty() {
                sort_rows(&mut rows, &self.sort_keys);
                let run_id = spill_run(ctx, &rows)?;
                run_ids.push(run_id);

                rows = Vec::new();
                bytes_used = 0;
            }
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

fn spill_run(ctx: &mut ExecContext, rows: &[Row]) -> Result<TempFileId> {
    let disk_reader = &mut *ctx.disk_reader;
    let disk_writer = &mut *ctx.disk_writer;
    let temp_storage = &mut *ctx.temp_storage;

    let mut writer = TempRunWriter::new(temp_storage)?;
    for row in rows {
        writer.append_row(row, disk_reader, disk_writer)?;
    }
    writer.finish(disk_reader, disk_writer)
}

fn sort_rows(rows: &mut [Row], sort_keys: &[SortKey]) {
    rows.sort_by(|a, b| compare_rows(sort_keys, a, b));
}

fn compare_rows(sort_keys: &[SortKey], a: &Row, b: &Row) -> Ordering {
    for key in sort_keys {
        let av = a
            .get(key.idx)
            .expect("sort key index out of bounds on left row");
        let bv = b
            .get(key.idx)
            .expect("sort key index out of bounds on right row");

        let ord = compare_data(av, bv).expect("incomparable values in ORDER BY");
        let ord = if key.ascending { ord } else { ord.reverse() };

        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn compare_data(left: &Data, right: &Data) -> Result<Ordering> {
    left.partial_cmp(right)
        .ok_or_else(|| anyhow!("cannot compare incompatible data types in sort"))
}

fn estimate_row_size(row: &Row) -> usize {
    use std::mem::size_of;

    let mut total = size_of::<Row>() + row.len() * size_of::<Data>();

    for value in row.values() {
        if let Data::String(s) = value {
            total += s.capacity();
        }
    }

    total
}

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

        let disk_reader = &mut *ctx.disk_reader;
        let disk_writer = &mut *ctx.disk_writer;
        let temp_storage = &*ctx.temp_storage;

        for (run_idx, run_id) in run_ids.into_iter().enumerate() {
            let mut reader = TempRunReader::new(temp_storage, run_id)?;
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