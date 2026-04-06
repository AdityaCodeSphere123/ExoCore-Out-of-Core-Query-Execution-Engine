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

/// Compute partition number directly from row values.
/// Avoids allocating a JoinKey (Vec + String clones) during the partitioning
/// phase where we only need the hash, not a comparable key.
fn partition_hash(row: &Row, indices: &[usize], num_partitions: usize) -> usize {
    let mut h = DefaultHasher::new();
    for &i in indices {
        match &row.values()[i] {
            Data::Int32(v) => v.hash(&mut h),
            Data::Int64(v) => v.hash(&mut h),
            Data::Float32(v) => v.to_bits().hash(&mut h),
            Data::Float64(v) => v.to_bits().hash(&mut h),
            Data::String(s) => s.hash(&mut h),
        }
    }
    (h.finish() as usize) % num_partitions
}

const MIN_HASH_JOIN_PARTITIONS: usize = 8;
const MAX_HASH_JOIN_PARTITIONS: usize = 32;
const TARGET_PARTITION_PAGES: usize = 256;

fn choose_num_partitions(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size();
    let target_bytes = TARGET_PARTITION_PAGES.saturating_mul(block_size).max(1);
    (ctx.sort_run_bytes / target_bytes).clamp(MIN_HASH_JOIN_PARTITIONS, MAX_HASH_JOIN_PARTITIONS)
}

/// Compute writer batch pages from available memory and partition count.
/// Larger batches → fewer, bigger contiguous extents → fewer seeks during probe.
fn choose_writer_batch(sort_run_bytes: usize, block_size: usize, num_partitions: usize) -> usize {
    let budget = sort_run_bytes / 4;
    let per_writer = budget / num_partitions.max(1);
    let batch_pages = per_writer / block_size.max(1);
    batch_pages.clamp(4, 64)
}

// ── entry point ───────────────────────────────────────────────────────────────

/// Classify predicates into equi-join keys and residual predicates.
/// Returns (left_key_indices, right_key_indices, extra_predicates).
fn split_join_predicates(
    left_schema: &RowSchema,
    right_schema: &RowSchema,
    predicates: &[Predicate],
) -> (Vec<usize>, Vec<usize>, Vec<Predicate>) {
    let mut left_key_indices: Vec<usize> = Vec::new();
    let mut right_key_indices: Vec<usize> = Vec::new();
    let mut extra_predicates: Vec<Predicate> = Vec::new();

    for pred in predicates {
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

    (left_key_indices, right_key_indices, extra_predicates)
}

/// Build a join operator without row-count hints.  Picks Grace Hash Join for
/// equi-join predicates; falls back to Block Nested Loop otherwise.
pub fn build_join<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
) -> Result<Box<dyn Operator + 'a>> {
    build_join_impl(left, right, predicates, None, None)
}

/// Same as `build_join` but accepts estimated row counts for both sides.
/// Used by the SPJ planner which has per-leaf statistics available.  The hints
/// are forwarded to `BlockNestedLoopJoinOperator` so it can choose which side
/// to spill (inner) and which to batch in memory (outer).
pub fn build_join_hinted<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
    left_rows_hint: f64,
    right_rows_hint: f64,
) -> Result<Box<dyn Operator + 'a>> {
    build_join_impl(left, right, predicates, Some(left_rows_hint), Some(right_rows_hint))
}

fn build_join_impl<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
    left_rows_hint: Option<f64>,
    right_rows_hint: Option<f64>,
) -> Result<Box<dyn Operator + 'a>> {
    let left_schema = left.schema().clone();
    let right_schema = right.schema().clone();
    let (left_key_indices, right_key_indices, extra_predicates) =
        split_join_predicates(&left_schema, &right_schema, predicates);

    if !left_key_indices.is_empty() {
        Ok(Box::new(HashJoinOperator::new(
            left,
            right,
            left_key_indices,
            right_key_indices,
            extra_predicates,
        )?))
    } else {
        Ok(Box::new(BlockNestedLoopJoinOperator::new(
            left,
            right,
            extra_predicates,
            left_rows_hint,
            right_rows_hint,
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
    /// Streaming reader over the probe partition, None before loaded.
    probe_reader: Option<TempRunReader>,
    /// Buffered output rows not yet returned to the caller.
    output_buf: Vec<Row>,
    /// True when the build side is the left table for the current partition.
    build_is_left: bool,
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
        let writer_batch = choose_writer_batch(
            ctx.sort_run_bytes,
            ctx.temp_storage.block_size(),
            num_partitions,
        );

        let mut left_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();
        let mut right_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();

        // Partition left — use allocation-free hashing.
        let mut left = self.left.take().expect("left already consumed");
        loop {
            let maybe = left.next(ctx)?;
            let row = match maybe {
                Some(r) => r,
                None => break,
            };
            let part = partition_hash(&row, &self.left_key_indices, num_partitions);
            if left_writers[part].is_none() {
                left_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    writer_batch,
                )?);
            }
            left_writers[part]
                .as_mut()
                .unwrap()
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
            let part = partition_hash(&row, &self.right_key_indices, num_partitions);
            if right_writers[part].is_none() {
                right_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    writer_batch,
                )?);
            }
            right_writers[part]
                .as_mut()
                .unwrap()
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
                        build_is_left: true,
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
                            return Ok(None);
                        }

                        // Pick the smaller side to build the hash table from.
                        let left_file = ps.left_parts[ps.current_part];
                        let right_file = ps.right_parts[ps.current_part];
                        let left_pages = ctx.temp_storage.num_pages(left_file)?;
                        let right_pages = ctx.temp_storage.num_pages(right_file)?;

                        let (build_file, probe_file, build_keys) = if left_pages <= right_pages {
                            ps.build_is_left = true;
                            (left_file, right_file, &self.left_key_indices as &[usize])
                        } else {
                            ps.build_is_left = false;
                            (right_file, left_file, &self.right_key_indices as &[usize])
                        };

                        // Build hash table from smaller partition.
                        let mut lr = TempRunReader::new(ctx.temp_storage, build_file)?;
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
                            let key = make_key(&row, build_keys);
                            ps.build_map.entry(key).or_default().push(row);
                        }

                        ps.probe_reader =
                            Some(TempRunReader::new(ctx.temp_storage, probe_file)?);
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
                            // This partition is done.  Delete both temp files
                            // immediately so disk space is reclaimed before the
                            // next partition is loaded, rather than holding all
                            // partition files open until the operator is dropped.
                            ctx.temp_storage
                                .delete_temp_file(ps.left_parts[ps.current_part])?;
                            ctx.temp_storage
                                .delete_temp_file(ps.right_parts[ps.current_part])?;
                            ps.current_part += 1;
                            ps.probe_reader = None;
                            ps.build_map.clear();
                            self.state = HashJoinState::Probing(ps);
                        }
                        Some(probe_row) => {
                            let probe_keys: &[usize] = if ps.build_is_left {
                                &self.right_key_indices
                            } else {
                                &self.left_key_indices
                            };
                            let key = make_key(&probe_row, probe_keys);
                            if let Some(build_rows) = ps.build_map.get(&key) {
                                for build_row in build_rows {
                                    // Maintain [left, right] column order regardless
                                    // of which side we built from.
                                    let merged = if ps.build_is_left {
                                        Row::merge(build_row, &probe_row)
                                    } else {
                                        Row::merge(&probe_row, build_row)
                                    };
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

// ── Block Nested-Loop Join (fallback for non-equi conditions) ───────────────

const MAX_BNLJ_OUTER_BATCH_PAGES: usize = 64;

fn choose_bnlj_outer_batch_bytes(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    let total_pages = (ctx.sort_run_bytes / block_size).max(1);
    let batch_pages = (total_pages / 8).max(1).min(MAX_BNLJ_OUTER_BATCH_PAGES);
    batch_pages * block_size
}

/// Out-of-core block nested loop join used when no equi-join predicate exists.
///
/// Strategy:
///   1. Identify the "inner" side (smaller, spilled once to temp storage) and
///      the "outer" side (larger, read in memory-sized batches).  When row-count
///      hints are provided, we spill whichever side has fewer rows; otherwise we
///      default to spilling left.  Spilling the smaller side minimises the
///      number of disk pages read per outer batch.
///   2. For each outer batch, re-scan the full inner file and evaluate
///      predicates against every (inner, outer) pair.
///
/// Output column order is always [original_left | original_right] regardless
/// of which side was chosen as inner.
pub struct BlockNestedLoopJoinOperator<'a> {
    /// Spilled side — scanned once per outer batch.
    inner: Option<Box<dyn Operator + 'a>>,
    /// Batched side — loaded into memory in chunks.
    outer: Option<Box<dyn Operator + 'a>>,
    /// True when the original *left* operand was chosen as inner.  Used to
    /// restore [left | right] column order when merging rows.
    inner_is_left: bool,
    resolved: Vec<ResolvedPredicate>,
    merged_schema: RowSchema,
    state: BlockNestedLoopState,
}

enum BlockNestedLoopState {
    NotStarted,
    NeedOuterBatch {
        inner_file: TempFileId,
    },
    ScanningInner {
        inner_file: TempFileId,
        outer_batch: Vec<Row>,
        inner_reader: TempRunReader,
        output_buf: Vec<Row>,
    },
    Done,
}

impl<'a> BlockNestedLoopJoinOperator<'a> {
    fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        predicates: Vec<Predicate>,
        left_rows_hint: Option<f64>,
        right_rows_hint: Option<f64>,
    ) -> Result<Self> {
        let left_schema = left.schema().clone();
        let right_schema = right.schema().clone();
        // merged_schema always preserves [left | right] column order so that
        // resolved predicate indices and output row layout are stable.
        let merged_schema = RowSchema::merge(&left_schema, &right_schema);
        let resolved = resolve_predicates(&merged_schema, &predicates)?;

        // Choose which side to spill.  Spilling the *smaller* side means we
        // read fewer pages on each rescan, directly reducing disk I/O.
        let inner_is_left = match (left_rows_hint, right_rows_hint) {
            (Some(l), Some(r)) => l <= r, // left is smaller → left is inner
            _ => true,                    // no hints → preserve original behaviour
        };
        let (inner, outer) = if inner_is_left {
            (left, right)
        } else {
            (right, left)
        };

        Ok(Self {
            inner: Some(inner),
            outer: Some(outer),
            inner_is_left,
            resolved,
            merged_schema,
            state: BlockNestedLoopState::NotStarted,
        })
    }

    fn spill_inner(&mut self, ctx: &mut ExecContext) -> Result<TempFileId> {
        // Use a larger write batch so the inner file lands in fewer extents,
        // reducing seeks on each outer-batch rescan.  Cap at 64 pages so the
        // pre-allocated write buffer fits comfortably in the memory budget.
        let block_size = ctx.temp_storage.block_size().max(1);
        let batch_pages = (ctx.sort_run_bytes / block_size).max(1).min(64);
        let mut writer = TempRunWriter::with_batch_pages(ctx.temp_storage, batch_pages)?;
        let mut inner = self.inner.take().expect("inner already consumed");
        loop {
            let row = match inner.next(ctx)? {
                Some(r) => r,
                None => break,
            };
            writer.append_row(&row, ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)?;
        }
        writer.finish(ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)
    }

    fn fill_outer_batch(&mut self, ctx: &mut ExecContext) -> Result<Vec<Row>> {
        let target_bytes = choose_bnlj_outer_batch_bytes(ctx);
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        let outer = self.outer.as_mut().expect("outer already consumed");

        loop {
            let row = match outer.next(ctx)? {
                Some(r) => r,
                None => break,
            };
            bytes += row.estimate_heap_size();
            batch.push(row);
            if bytes >= target_bytes {
                break;
            }
        }
        Ok(batch)
    }
}

impl<'a> Operator for BlockNestedLoopJoinOperator<'a> {
    fn schema(&self) -> &RowSchema {
        &self.merged_schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        loop {
            match std::mem::replace(&mut self.state, BlockNestedLoopState::Done) {
                BlockNestedLoopState::NotStarted => {
                    let inner_file = self.spill_inner(ctx)?;
                    self.state = BlockNestedLoopState::NeedOuterBatch { inner_file };
                }

                BlockNestedLoopState::Done => return Ok(None),

                BlockNestedLoopState::NeedOuterBatch { inner_file } => {
                    let outer_batch = self.fill_outer_batch(ctx)?;
                    if outer_batch.is_empty() {
                        ctx.temp_storage.delete_temp_file(inner_file)?;
                        self.state = BlockNestedLoopState::Done;
                        return Ok(None);
                    }

                    let inner_reader = TempRunReader::new(ctx.temp_storage, inner_file)?;
                    self.state = BlockNestedLoopState::ScanningInner {
                        inner_file,
                        outer_batch,
                        inner_reader,
                        output_buf: Vec::new(),
                    };
                }

                BlockNestedLoopState::ScanningInner {
                    inner_file,
                    outer_batch,
                    mut inner_reader,
                    mut output_buf,
                } => {
                    if let Some(row) = output_buf.pop() {
                        self.state = BlockNestedLoopState::ScanningInner {
                            inner_file,
                            outer_batch,
                            inner_reader,
                            output_buf,
                        };
                        return Ok(Some(row));
                    }

                    let maybe_inner = inner_reader.next_row(
                        ctx.temp_storage,
                        &mut *ctx.disk_reader,
                        &mut *ctx.disk_writer,
                    )?;

                    match maybe_inner {
                        None => {
                            // Inner exhausted; get the next outer batch.
                            self.state = BlockNestedLoopState::NeedOuterBatch { inner_file };
                        }
                        Some(inner_row) => {
                            for outer_row in outer_batch.iter().rev() {
                                // Restore [original_left | original_right] column order.
                                let merged = if self.inner_is_left {
                                    Row::merge(&inner_row, outer_row)
                                } else {
                                    Row::merge(outer_row, &inner_row)
                                };
                                if eval_resolved(&merged, &self.resolved)? {
                                    output_buf.push(merged);
                                }
                            }
                            self.state = BlockNestedLoopState::ScanningInner {
                                inner_file,
                                outer_batch,
                                inner_reader,
                                output_buf,
                            };
                        }
                    }
                }
            }
        }
    }
}
