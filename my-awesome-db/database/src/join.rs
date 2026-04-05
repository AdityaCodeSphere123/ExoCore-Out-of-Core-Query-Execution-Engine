use anyhow::Result;
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use common::Data;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::filter::{eval_resolved, resolve_predicates, ResolvedPredicate};
use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};
use crate::temp_storage::{TempFileId, TempRunReader, TempRunWriter};

// ── join key ─────────────────────────────────────────────────────────────────

/// A row's join-key values, comparable and hashable.
#[derive(Hash, Eq, PartialEq)]
struct JoinKey(Vec<KeyField>);

#[derive(Hash, Eq, PartialEq)]
enum KeyField {
    I32(i32),
    I64(i64),
    F32(u32), // stored as bits so that bit-equal floats hash equally
    F64(u64),
    Str(String),
}

fn to_key_field(d: &Data) -> KeyField {
    match d {
        Data::Int32(v) => KeyField::I32(*v),
        Data::Int64(v) => KeyField::I64(*v),
        Data::Float32(v) => KeyField::F32(v.to_bits()),
        Data::Float64(v) => KeyField::F64(v.to_bits()),
        Data::String(s) => KeyField::Str(s.clone()),
    }
}

fn make_key(row: &Row, indices: &[usize]) -> JoinKey {
    JoinKey(indices.iter().map(|&i| to_key_field(&row.values()[i])).collect())
}

fn partition_of(key: &JoinKey, n: usize) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % n
}

// Keep hash-join partitioning conservative. The old code scaled up to 256
// partitions from sort_run_bytes, which caused lots of live writers and empty
// or one-sided temp files. This keeps memory bounded while still spreading
// data enough for probing.
const MIN_HASH_JOIN_PARTITIONS: usize = 8;
const MAX_HASH_JOIN_PARTITIONS: usize = 32;
const TARGET_PARTITION_PAGES: usize = 256;
const JOIN_WRITER_BATCH: usize = 4;

fn choose_num_partitions(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size();
    let target_bytes = TARGET_PARTITION_PAGES.saturating_mul(block_size).max(1);
    (ctx.sort_run_bytes / target_bytes).clamp(MIN_HASH_JOIN_PARTITIONS, MAX_HASH_JOIN_PARTITIONS)
}

// ── entry point ───────────────────────────────────────────────────────────────

/// Inspect predicates and build a Grace Hash Join when there is at least one
/// equi-join condition (col_from_left = col_from_right).  Falls back to a
/// nested-loop join otherwise.
pub fn build_join<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
) -> Result<Box<dyn Operator + 'a>> {
    let left_schema = left.schema().clone();
    let right_schema = right.schema().clone();

    let mut left_key_indices: Vec<usize> = Vec::new();
    let mut right_key_indices: Vec<usize> = Vec::new();
    let mut extra_predicates: Vec<Predicate> = Vec::new();

    for pred in predicates {
        // An equi-join predicate is EQ where one operand is in the left
        // schema and the other is in the right schema.
        if let (ComparisionOperator::EQ, ComparisionValue::Column(other_col)) =
            (&pred.operator, &pred.value)
        {
            let l_in_left = left_schema.index_of(&pred.column_name);
            let r_in_right = right_schema.index_of(other_col);
            let l_in_right = right_schema.index_of(&pred.column_name);
            let r_in_left = left_schema.index_of(other_col);

            if let (Some(li), Some(ri)) = (l_in_left, r_in_right) {
                left_key_indices.push(li);
                right_key_indices.push(ri);
                continue;
            }
            if let (Some(ri), Some(li)) = (l_in_right, r_in_left) {
                left_key_indices.push(li);
                right_key_indices.push(ri);
                continue;
            }
        }
        extra_predicates.push(pred.clone());
    }

    if !left_key_indices.is_empty() {
        Ok(Box::new(HashJoinOperator::new(
            left,
            right,
            left_key_indices,
            right_key_indices,
            extra_predicates,
        )?))
    } else {
        Ok(Box::new(CrossJoinOperator::new(
            left,
            right,
            extra_predicates,
        )?))
    }
}

// ── Grace Hash Join ───────────────────────────────────────────────────────────

pub struct HashJoinOperator<'a> {
    left: Option<Box<dyn Operator + 'a>>,
    right: Option<Box<dyn Operator + 'a>>,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    resolved_extra: Vec<ResolvedPredicate>,
    left_schema: RowSchema,
    merged_schema: RowSchema,
    state: HashJoinState,
}

enum HashJoinState {
    NotStarted,
    Probing(ProbingState),
    Done,
}

struct ProbingState {
    left_parts: Vec<TempFileId>,
    right_parts: Vec<TempFileId>,
    current_part: usize,
    /// Build-side hash table for the current partition.
    build_map: HashMap<JoinKey, Vec<Row>>,
    /// Streaming reader over the right (probe) partition, None before loaded.
    probe_reader: Option<TempRunReader>,
    /// Buffered output rows not yet returned to the caller.
    output_buf: Vec<Row>,
}

impl<'a> HashJoinOperator<'a> {
    fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        extra_predicates: Vec<Predicate>,
    ) -> Result<Self> {
        let left_schema = left.schema().clone();
        let right_schema = right.schema().clone();
        let merged_schema = RowSchema::merge(&left_schema, &right_schema);
        let resolved_extra = resolve_predicates(&merged_schema, &extra_predicates)?;
        Ok(Self {
            left: Some(left),
            right: Some(right),
            left_key_indices,
            right_key_indices,
            resolved_extra,
            left_schema,
            merged_schema,
            state: HashJoinState::NotStarted,
        })
    }

    /// Partition both inputs into temp files, but create writers lazily so we
    /// never pay memory or temp-file overhead for untouched partitions.
    ///
    /// We also only keep partitions where both sides are non-empty. One-sided
    /// partitions can never produce output, so we discard them before probing.
    fn partition_inputs(
        &mut self,
        ctx: &mut ExecContext,
    ) -> Result<(Vec<TempFileId>, Vec<TempFileId>)> {
        let num_partitions = choose_num_partitions(ctx);

        let mut left_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();
        let mut right_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();

        // Partition left.
        let mut left = self.left.take().expect("left already consumed");
        loop {
            let maybe = left.next(ctx)?;
            let row = match maybe {
                Some(r) => r,
                None => break,
            };
            let key = make_key(&row, &self.left_key_indices);
            let part = partition_of(&key, num_partitions);
            if left_writers[part].is_none() {
                left_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    JOIN_WRITER_BATCH,
                )?);
            }
            left_writers[part]
                .as_mut()
                .expect("left partition writer should exist")
                .append_row(
                    &row,
                    ctx.temp_storage,
                    &mut *ctx.disk_reader,
                    &mut *ctx.disk_writer,
                )?;
        }
        drop(left);

        // Partition right.
        let mut right = self.right.take().expect("right already consumed");
        loop {
            let maybe = right.next(ctx)?;
            let row = match maybe {
                Some(r) => r,
                None => break,
            };
            let key = make_key(&row, &self.right_key_indices);
            let part = partition_of(&key, num_partitions);
            if right_writers[part].is_none() {
                right_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    JOIN_WRITER_BATCH,
                )?);
            }
            right_writers[part]
                .as_mut()
                .expect("right partition writer should exist")
                .append_row(
                    &row,
                    ctx.temp_storage,
                    &mut *ctx.disk_reader,
                    &mut *ctx.disk_writer,
                )?;
        }
        drop(right);

        let mut left_parts = Vec::new();
        let mut right_parts = Vec::new();

        for part in 0..num_partitions {
            match (left_writers[part].take(), right_writers[part].take()) {
                (Some(left_writer), Some(right_writer)) => {
                    let left_file = left_writer.finish(
                        ctx.temp_storage,
                        &mut *ctx.disk_reader,
                        &mut *ctx.disk_writer,
                    )?;
                    let right_file = right_writer.finish(
                        ctx.temp_storage,
                        &mut *ctx.disk_reader,
                        &mut *ctx.disk_writer,
                    )?;
                    left_parts.push(left_file);
                    right_parts.push(right_file);
                }
                (Some(left_writer), None) => {
                    // This partition can never join. Drop any unwritten state and
                    // remove its temp-file metadata so we never probe it later.
                    let file_id = left_writer.file_id();
                    drop(left_writer);
                    ctx.temp_storage.delete_temp_file(file_id)?;
                }
                (None, Some(right_writer)) => {
                    let file_id = right_writer.file_id();
                    drop(right_writer);
                    ctx.temp_storage.delete_temp_file(file_id)?;
                }
                (None, None) => {}
            }
        }

        Ok((left_parts, right_parts))
    }
}

impl<'a> Operator for HashJoinOperator<'a> {
    fn schema(&self) -> &RowSchema {
        &self.merged_schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        // We use std::mem::replace to temporarily own `self.state` so we can
        // also access `self.left_key_indices`, `self.merged_schema`, etc.
        loop {
            match std::mem::replace(&mut self.state, HashJoinState::Done) {
                HashJoinState::NotStarted => {
                    let (lp, rp) = self.partition_inputs(ctx)?;
                    self.state = HashJoinState::Probing(ProbingState {
                        left_parts: lp,
                        right_parts: rp,
                        current_part: 0,
                        build_map: HashMap::new(),
                        probe_reader: None,
                        output_buf: Vec::new(),
                    });
                }

                HashJoinState::Done => return Ok(None),

                HashJoinState::Probing(mut ps) => {
                    // Return any rows already matched but not yet emitted.
                    if !ps.output_buf.is_empty() {
                        let row = ps.output_buf.pop().unwrap();
                        self.state = HashJoinState::Probing(ps);
                        return Ok(Some(row));
                    }

                    // Load the next partition if we haven't started it yet.
                    if ps.probe_reader.is_none() {
                        if ps.current_part >= ps.left_parts.len() {
                            // All partitions exhausted — state stays Done.
                            return Ok(None);
                        }

                        // Build hash table from left partition.
                        let left_file = ps.left_parts[ps.current_part];
                        let mut lr = TempRunReader::new(ctx.temp_storage, left_file)?;
                        ps.build_map.clear();
                        loop {
                            let maybe = lr.next_row(
                                ctx.temp_storage,
                                &mut *ctx.disk_reader,
                                &mut *ctx.disk_writer,
                            )?;
                            let row = match maybe {
                                Some(r) => r,
                                None => break,
                            };
                            let key = make_key(&row, &self.left_key_indices);
                            ps.build_map.entry(key).or_default().push(row);
                        }

                        // Open streaming reader for right (probe) partition.
                        let right_file = ps.right_parts[ps.current_part];
                        ps.probe_reader = Some(TempRunReader::new(ctx.temp_storage, right_file)?);
                    }

                    // Fetch next probe row.
                    let probe_row = {
                        let reader = ps.probe_reader.as_mut().unwrap();
                        reader.next_row(
                            ctx.temp_storage,
                            &mut *ctx.disk_reader,
                            &mut *ctx.disk_writer,
                        )?
                    };

                    match probe_row {
                        None => {
                            // This partition is done; advance.
                            ps.current_part += 1;
                            ps.probe_reader = None;
                            ps.build_map.clear();
                            self.state = HashJoinState::Probing(ps);
                        }
                        Some(right_row) => {
                            let key = make_key(&right_row, &self.right_key_indices);
                            if let Some(left_rows) = ps.build_map.get(&key) {
                                for left_row in left_rows {
                                    let merged = Row::merge(left_row, &right_row);
                                    if eval_resolved(&merged, &self.resolved_extra)? {
                                        ps.output_buf.push(merged);
                                    }
                                }
                            }
                            self.state = HashJoinState::Probing(ps);
                        }
                    }
                }
            }
        }
    }
}

// ── Cross / Nested-Loop Join (fallback for non-equi conditions) ───────────────

/// Out-of-core nested loop join used when no equi-join predicate exists.
///
/// Strategy:
///   1. On first call, spill **all left rows** to a temp file.
///   2. For each right row, replay the left temp file and emit merged rows
///      that satisfy all predicates.
///
/// Memory: 2 × block_size (one reader page buffer per side) + one row.
pub struct CrossJoinOperator<'a> {
    left: Option<Box<dyn Operator + 'a>>,
    right: Option<Box<dyn Operator + 'a>>,
    resolved: Vec<ResolvedPredicate>,
    merged_schema: RowSchema,
    state: CrossState,
}

enum CrossState {
    NotStarted,
    Running {
        left_file: TempFileId,
        /// Current right row being matched against all left rows.
        right_row: Row,
        left_reader: TempRunReader,
    },
    /// Left file ready; need to fetch the next right row.
    FetchRight {
        left_file: TempFileId,
    },
    Done,
}

impl<'a> CrossJoinOperator<'a> {
    fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        predicates: Vec<Predicate>,
    ) -> Result<Self> {
        let left_schema = left.schema().clone();
        let right_schema = right.schema().clone();
        let merged_schema = RowSchema::merge(&left_schema, &right_schema);
        let resolved = resolve_predicates(&merged_schema, &predicates)?;
        Ok(Self {
            left: Some(left),
            right: Some(right),
            resolved,
            merged_schema,
            state: CrossState::NotStarted,
        })
    }

    fn spill_left(&mut self, ctx: &mut ExecContext) -> Result<TempFileId> {
        let mut writer = TempRunWriter::new(ctx.temp_storage)?;
        let mut left = self.left.take().expect("left already consumed");
        loop {
            let maybe = left.next(ctx)?;
            let row = match maybe {
                Some(r) => r,
                None => break,
            };
            writer.append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?;
        }
        writer.finish(ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)
    }
}

impl<'a> Operator for CrossJoinOperator<'a> {
    fn schema(&self) -> &RowSchema {
        &self.merged_schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        loop {
            match std::mem::replace(&mut self.state, CrossState::Done) {
                CrossState::NotStarted => {
                    let left_file = self.spill_left(ctx)?;
                    self.state = CrossState::FetchRight { left_file };
                }

                CrossState::Done => return Ok(None),

                CrossState::FetchRight { left_file } => {
                    let right = self.right.as_mut().expect("right already consumed");
                    let maybe = right.next(ctx)?;
                    match maybe {
                        None => {
                            // state stays Done
                            return Ok(None);
                        }
                        Some(right_row) => {
                            let left_reader = TempRunReader::new(ctx.temp_storage, left_file)?;
                            self.state = CrossState::Running {
                                left_file,
                                right_row,
                                left_reader,
                            };
                        }
                    }
                }

                CrossState::Running {
                    left_file,
                    right_row,
                    mut left_reader,
                } => {
                    let maybe = left_reader.next_row(
                        ctx.temp_storage,
                        &mut *ctx.disk_reader,
                        &mut *ctx.disk_writer,
                    )?;
                    match maybe {
                        None => {
                            // Left exhausted for this right row; fetch next right row.
                            self.state = CrossState::FetchRight { left_file };
                        }
                        Some(left_row) => {
                            let merged = Row::merge(&left_row, &right_row);
                            // Put state back before potentially returning.
                            self.state = CrossState::Running {
                                left_file,
                                right_row,
                                left_reader,
                            };
                            if eval_resolved(&merged, &self.resolved)? {
                                return Ok(Some(merged));
                            }
                        }
                    }
                }
            }
        }
    }
}
