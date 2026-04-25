use anyhow::Result;
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use common::Data;
use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::filter::{eval_resolved, resolve_predicates, ResolvedPredicate};
use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};
use crate::temp_storage::{TempFileId, TempRunReader, TempRunWriter};

type FastHashMap<K, V> = std::collections::HashMap<K, V, std::hash::BuildHasherDefault<FastHasher>>;

#[derive(Default)]
pub struct FastHasher {
    state: u64,
}

impl std::hash::Hasher for FastHasher {
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

    /// Unrolled 4-byte FNV — eliminates loop overhead and the `to_le_bytes()`
    /// stack allocation.  Called on every integer join-key hash during probing.
    #[inline]
    fn write_u32(&mut self, i: u32) {
        const P: u64 = 0x100000001b3;
        let mut h = if self.state == 0 { 0xcbf29ce484222325u64 } else { self.state };
        h ^= (i & 0xff) as u64;          h = h.wrapping_mul(P);
        h ^= ((i >> 8) & 0xff) as u64;   h = h.wrapping_mul(P);
        h ^= ((i >> 16) & 0xff) as u64;  h = h.wrapping_mul(P);
        h ^= ((i >> 24) & 0xff) as u64;  h = h.wrapping_mul(P);
        self.state = h;
    }

    /// Unrolled 8-byte FNV.
    #[inline]
    fn write_u64(&mut self, i: u64) {
        const P: u64 = 0x100000001b3;
        let mut h = if self.state == 0 { 0xcbf29ce484222325u64 } else { self.state };
        h ^= (i & 0xff) as u64;          h = h.wrapping_mul(P);
        h ^= ((i >> 8) & 0xff) as u64;   h = h.wrapping_mul(P);
        h ^= ((i >> 16) & 0xff) as u64;  h = h.wrapping_mul(P);
        h ^= ((i >> 24) & 0xff) as u64;  h = h.wrapping_mul(P);
        h ^= ((i >> 32) & 0xff) as u64;  h = h.wrapping_mul(P);
        h ^= ((i >> 40) & 0xff) as u64;  h = h.wrapping_mul(P);
        h ^= ((i >> 48) & 0xff) as u64;  h = h.wrapping_mul(P);
        h ^= ((i >> 56) & 0xff) as u64;  h = h.wrapping_mul(P);
        self.state = h;
    }

    #[inline]
    fn write_usize(&mut self, i: usize) { self.write_u64(i as u64); }
    #[inline]
    fn write_i32(&mut self, i: i32) { self.write_u32(i as u32); }
    #[inline]
    fn write_i64(&mut self, i: i64) { self.write_u64(i as u64); }
    fn finish(&self) -> u64 { self.state }
}

// ── join key ─────────────────────────────────────────────────────────────────

/// A row's join-key values, comparable and hashable.
#[derive(Eq, PartialEq, Clone)]
enum JoinKey {
    One(KeyField),
    Many(Vec<KeyField>),
}

impl std::hash::Hash for JoinKey {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        match self {
            JoinKey::One(kf) => kf.hash(h),
            JoinKey::Many(kfs) => {
                for kf in kfs {
                    kf.hash(h);
                }
            }
        }
    }
}

#[derive(Eq, PartialEq, Clone)]
enum KeyField {
    I32(i32),
    I64(i64),
    F32(u32), // stored as bits so that bit-equal floats hash equally
    F64(u64),
    Str(String),
}

impl std::hash::Hash for KeyField {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        match self {
            KeyField::I32(v) => h.write_i32(*v),
            KeyField::I64(v) => h.write_i64(*v),
            KeyField::F32(v) => h.write_u32(*v),
            KeyField::F64(v) => h.write_u64(*v),
            KeyField::Str(s) => s.hash(h),
        }
    }
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
    match indices {
        [idx] => JoinKey::One(to_key_field(&row.values()[*idx])),
        _ => JoinKey::Many(indices.iter().map(|&i| to_key_field(&row.values()[i])).collect()),
    }
}

fn estimate_join_key_heap_size(key: &JoinKey) -> usize {
    use std::mem::size_of;

    match key {
        JoinKey::One(field) => {
            let mut total = size_of::<JoinKey>();
            if let KeyField::Str(s) = field {
                total += s.capacity() + 16;
            }
            total
        }
        JoinKey::Many(fields) => {
            let mut total = size_of::<JoinKey>() + fields.len() * size_of::<KeyField>() + 16;
            for field in fields {
                if let KeyField::Str(s) = field {
                    total += s.capacity() + 16;
                }
            }
            total
        }
    }
}

/// Compute partition number directly from row values.
/// Avoids allocating a JoinKey (Vec + String clones) during the partitioning
/// phase where we only need the hash, not a comparable key.
fn partition_hash_with_salt(
    row: &Row,
    indices: &[usize],
    num_partitions: usize,
    salt: u64,
) -> usize {
    let mut h = DefaultHasher::new();
    salt.hash(&mut h);
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
const HASH_JOIN_MAX_REPARTITION_DEPTH: usize = 3;
const HASH_JOIN_BUILD_READER_PAGES: usize = 128;
const HASH_JOIN_PROBE_READER_PAGES: usize = 128;
const HASH_JOIN_BUILD_MISC_RESERVE_BYTES: usize = 256 * 1024;
const HASH_JOIN_ENTRY_OVERHEAD_BYTES: usize = 64;

// ── Bloom filter ──────────────────────────────────────────────────────────────

/// Size of the per-join Bloom filter in bytes.  2 MB → 16 M bits.
/// At 7 probes this gives FPR ≈ (1-e^(-7n/16M))^7.
///   n=135K (typical Q3 build side): FPR < 10^-14  → effectively 0
///   n=6M   (worst-case same-table): FPR ≈ 15%     → 85% of probe rows skipped
const BLOOM_FILTER_BYTES: usize = 2 * 1024 * 1024;

struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
}

impl BloomFilter {
    fn new() -> Self {
        let num_bits = BLOOM_FILTER_BYTES * 8;
        let words = (num_bits + 63) / 64;
        Self {
            bits: vec![0u64; words],
            num_bits,
        }
    }

    fn insert(&mut self, hash: u64) {
        let h2 = hash.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        let m = self.num_bits;
        let mut h = hash;
        for _ in 0..7usize {
            let bit = (h as usize) % m;
            self.bits[bit >> 6] |= 1u64 << (bit & 63);
            h = h.wrapping_add(h2);
        }
    }

    fn may_contain(&self, hash: u64) -> bool {
        let h2 = hash.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        let m = self.num_bits;
        let mut h = hash;
        for _ in 0..7usize {
            let bit = (h as usize) % m;
            if (self.bits[bit >> 6] >> (bit & 63)) & 1 == 0 {
                return false;
            }
            h = h.wrapping_add(h2);
        }
        true
    }
}

/// Hash the join-key fields of `row` at `indices` into a single u64.
/// Uses the same logic for left and right rows so that equal key values
/// always hash to the same number.
fn compute_join_key_hash(row: &Row, indices: &[usize]) -> u64 {
    let mut h = DefaultHasher::new();
    for &i in indices {
        match &row.values()[i] {
            Data::Int32(v)  => v.hash(&mut h),
            Data::Int64(v)  => v.hash(&mut h),
            Data::Float32(v) => v.to_bits().hash(&mut h),
            Data::Float64(v) => v.to_bits().hash(&mut h),
            Data::String(s) => s.hash(&mut h),
        }
    }
    h.finish()
}

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
    batch_pages.clamp(4, 128)
}

fn choose_hash_build_budget_bytes(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    let reader_reserve = (HASH_JOIN_BUILD_READER_PAGES + HASH_JOIN_PROBE_READER_PAGES)
        .saturating_mul(block_size);
    ctx.sort_run_bytes
        .saturating_sub(reader_reserve)
        .saturating_sub(HASH_JOIN_BUILD_MISC_RESERVE_BYTES)
        .max(block_size)
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

#[derive(Clone, Copy)]
struct PartitionTask {
    left_file: TempFileId,
    right_file: TempFileId,
    depth: usize,
    salt: u64,
}

enum ActivePartition {
    Hash {
        task: PartitionTask,
        build_map: HashMap<JoinKey, Vec<Row>>,
        probe_reader: TempRunReader,
        build_is_left: bool,
    },
    NestedLoop {
        task: PartitionTask,
        inner_is_left: bool,
        inner_file: TempFileId,
        outer_reader: TempRunReader,
        outer_batch: Vec<Row>,
        inner_reader: TempRunReader,
    },
}

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
    pending_tasks: VecDeque<PartitionTask>,
    active: Option<ActivePartition>,
    output_buf: Vec<Row>,
}

enum PreparedPartition {
    Active(ActivePartition),
    Repartition(VecDeque<PartitionTask>),
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
    fn partition_inputs(&mut self, ctx: &mut ExecContext) -> Result<VecDeque<PartitionTask>> {
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

        // Build a Bloom filter from all left-side join keys.  When we stream
        // the right (probe) side, any row whose key hash is absent in the
        // filter is guaranteed to have no matching left row — skip it
        // entirely and avoid writing it to a partition file at all.
        // This is the single biggest I/O win for queries like Q3/Q5/Q20/Q21
        // where the probe side is lineitem (1333 pages) but only a small
        // fraction of its rows can ever join with the filtered build side.
        let mut bloom = BloomFilter::new();

        let mut left = self.left.take().expect("left already consumed");
        loop {
            let maybe = left.next(ctx)?;
            let row = match maybe {
                Some(r) => r,
                None => break,
            };
            bloom.insert(compute_join_key_hash(&row, &self.left_key_indices));
            let part = partition_hash_with_salt(&row, &self.left_key_indices, num_partitions, 0);
            if left_writers[part].is_none() {
                left_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    writer_batch,
                )?);
            }
            left_writers[part].as_mut().unwrap().append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?;
        }
        drop(left);

        let mut right = self.right.take().expect("right already consumed");
        loop {
            let maybe = right.next(ctx)?;
            let row = match maybe {
                Some(r) => r,
                None => break,
            };
            // Bloom filter early-exit: skip rows that definitely have no match.
            let key_hash = compute_join_key_hash(&row, &self.right_key_indices);
            if !bloom.may_contain(key_hash) {
                continue;
            }
            let part = partition_hash_with_salt(&row, &self.right_key_indices, num_partitions, 0);
            if right_writers[part].is_none() {
                right_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    writer_batch,
                )?);
            }
            right_writers[part].as_mut().unwrap().append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?;
        }
        drop(right);
        drop(bloom);

        let mut tasks = VecDeque::new();

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
                    tasks.push_back(PartitionTask {
                        left_file,
                        right_file,
                        depth: 0,
                        salt: 0,
                    });
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

        Ok(tasks)
    }

    fn repartition_task(
        &self,
        ctx: &mut ExecContext,
        task: PartitionTask,
    ) -> Result<VecDeque<PartitionTask>> {
        let num_partitions = choose_num_partitions(ctx);
        let writer_batch = choose_writer_batch(
            ctx.sort_run_bytes,
            ctx.temp_storage.block_size(),
            num_partitions,
        );
        let next_depth = task.depth + 1;
        let next_salt = task
            .salt
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(next_depth as u64 + 1);

        let mut left_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();
        let mut right_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();

        let mut left_reader = TempRunReader::with_batch_pages(
            ctx.temp_storage,
            task.left_file,
            HASH_JOIN_BUILD_READER_PAGES,
        )?;
        while let Some(row) = left_reader.next_row(
            ctx.temp_storage,
            &mut *ctx.disk_reader,
            &mut *ctx.disk_writer,
        )? {
            let part = partition_hash_with_salt(
                &row,
                &self.left_key_indices,
                num_partitions,
                next_salt,
            );
            if left_writers[part].is_none() {
                left_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    writer_batch,
                )?);
            }
            left_writers[part].as_mut().unwrap().append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?;
        }
        drop(left_reader);

        let mut right_reader = TempRunReader::with_batch_pages(
            ctx.temp_storage,
            task.right_file,
            HASH_JOIN_BUILD_READER_PAGES,
        )?;
        while let Some(row) = right_reader.next_row(
            ctx.temp_storage,
            &mut *ctx.disk_reader,
            &mut *ctx.disk_writer,
        )? {
            let part = partition_hash_with_salt(
                &row,
                &self.right_key_indices,
                num_partitions,
                next_salt,
            );
            if right_writers[part].is_none() {
                right_writers[part] = Some(TempRunWriter::with_batch_pages(
                    ctx.temp_storage,
                    writer_batch,
                )?);
            }
            right_writers[part].as_mut().unwrap().append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?;
        }
        drop(right_reader);

        ctx.temp_storage.delete_temp_file(task.left_file)?;
        ctx.temp_storage.delete_temp_file(task.right_file)?;

        let mut children = VecDeque::new();
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
                    children.push_back(PartitionTask {
                        left_file,
                        right_file,
                        depth: next_depth,
                        salt: next_salt,
                    });
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

        Ok(children)
    }

    fn cleanup_task(&self, ctx: &mut ExecContext, task: PartitionTask) -> Result<()> {
        ctx.temp_storage.delete_temp_file(task.left_file)?;
        ctx.temp_storage.delete_temp_file(task.right_file)?;
        Ok(())
    }

    fn prepare_nested_loop_partition(
        &self,
        ctx: &mut ExecContext,
        task: PartitionTask,
    ) -> Result<PreparedPartition> {
        let left_pages = ctx.temp_storage.num_pages(task.left_file)?;
        let right_pages = ctx.temp_storage.num_pages(task.right_file)?;
        let (inner_file, outer_file, inner_is_left) = if left_pages <= right_pages {
            (task.left_file, task.right_file, true)
        } else {
            (task.right_file, task.left_file, false)
        };

        let mut outer_reader = TempRunReader::with_batch_pages(
            ctx.temp_storage,
            outer_file,
            HASH_JOIN_PROBE_READER_PAGES,
        )?;
        let outer_batch = fill_temp_outer_batch(&mut outer_reader, ctx)?;
        if outer_batch.is_empty() {
            self.cleanup_task(ctx, task)?;
            return Ok(PreparedPartition::Repartition(VecDeque::new()));
        }

        let inner_reader = TempRunReader::with_batch_pages(
            ctx.temp_storage,
            inner_file,
            HASH_JOIN_BUILD_READER_PAGES,
        )?;

        Ok(PreparedPartition::Active(ActivePartition::NestedLoop {
            task,
            inner_is_left,
            inner_file,
            outer_reader,
            outer_batch,
            inner_reader,
        }))
    }

    fn prepare_task(
        &self,
        ctx: &mut ExecContext,
        task: PartitionTask,
    ) -> Result<PreparedPartition> {
        let build_budget = choose_hash_build_budget_bytes(ctx);
        let left_pages = ctx.temp_storage.num_pages(task.left_file)?;
        let right_pages = ctx.temp_storage.num_pages(task.right_file)?;
        let (build_file, probe_file, build_keys, build_is_left, build_pages) = if left_pages <= right_pages {
            (
                task.left_file,
                task.right_file,
                &self.left_key_indices as &[usize],
                true,
                left_pages,
            )
        } else {
            (
                task.right_file,
                task.left_file,
                &self.right_key_indices as &[usize],
                false,
                right_pages,
            )
        };

        let block_size = ctx.temp_storage.block_size().max(1);
        if build_pages.saturating_mul(block_size as u64) as usize > build_budget.saturating_mul(2)
            && task.depth < HASH_JOIN_MAX_REPARTITION_DEPTH
        {
            return Ok(PreparedPartition::Repartition(self.repartition_task(ctx, task)?));
        }

        let mut build_reader = TempRunReader::with_batch_pages(
            ctx.temp_storage,
            build_file,
            HASH_JOIN_BUILD_READER_PAGES,
        )?;
        let mut build_map: HashMap<JoinKey, Vec<Row>> = HashMap::new();
        let mut bytes_used = 0usize;

        while let Some(row) = build_reader.next_row(
            ctx.temp_storage,
            &mut *ctx.disk_reader,
            &mut *ctx.disk_writer,
        )? {
            let key = make_key(&row, build_keys);
            let row_bytes = row.estimate_heap_size();
            let key_bytes = estimate_join_key_heap_size(&key);
            let projected = bytes_used
                .saturating_add(row_bytes)
                .saturating_add(key_bytes)
                .saturating_add(HASH_JOIN_ENTRY_OVERHEAD_BYTES);

            if projected > build_budget {
                drop(build_reader);
                drop(build_map);
                if task.depth < HASH_JOIN_MAX_REPARTITION_DEPTH {
                    return Ok(PreparedPartition::Repartition(self.repartition_task(ctx, task)?));
                }
                return self.prepare_nested_loop_partition(ctx, task);
            }

            bytes_used = projected;
            build_map.entry(key).or_default().push(row);
        }
        drop(build_reader);

        let probe_reader = TempRunReader::with_batch_pages(
            ctx.temp_storage,
            probe_file,
            HASH_JOIN_PROBE_READER_PAGES,
        )?;

        Ok(PreparedPartition::Active(ActivePartition::Hash {
            task,
            build_map,
            probe_reader,
            build_is_left,
        }))
    }
}

fn fill_temp_outer_batch(
    reader: &mut TempRunReader,
    ctx: &mut ExecContext,
) -> Result<Vec<Row>> {
    let target_bytes = choose_bnlj_outer_batch_bytes(ctx);
    let mut batch = Vec::new();
    let mut bytes = 0usize;

    loop {
        let row = match reader.next_row(
            ctx.temp_storage,
            &mut *ctx.disk_reader,
            &mut *ctx.disk_writer,
        )? {
            Some(r) => r,
            None => break,
        };
        bytes = bytes.saturating_add(row.estimate_heap_size());
        batch.push(row);
        if bytes >= target_bytes {
            break;
        }
    }

    Ok(batch)
}

impl<'a> Operator for HashJoinOperator<'a> {
    fn schema(&self) -> &RowSchema {
        &self.merged_schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        loop {
            match std::mem::replace(&mut self.state, HashJoinState::Done) {
                HashJoinState::NotStarted => {
                    let tasks = self.partition_inputs(ctx)?;
                    self.state = HashJoinState::Probing(ProbingState {
                        pending_tasks: tasks,
                        active: None,
                        output_buf: Vec::new(),
                    });
                }

                HashJoinState::Done => return Ok(None),

                HashJoinState::Probing(mut ps) => {
                    if let Some(row) = ps.output_buf.pop() {
                        self.state = HashJoinState::Probing(ps);
                        return Ok(Some(row));
                    }

                    while ps.active.is_none() {
                        let task = match ps.pending_tasks.pop_front() {
                            Some(task) => task,
                            None => return Ok(None),
                        };

                        match self.prepare_task(ctx, task)? {
                            PreparedPartition::Active(active) => ps.active = Some(active),
                            PreparedPartition::Repartition(mut children) => {
                                while let Some(child) = children.pop_back() {
                                    ps.pending_tasks.push_front(child);
                                }
                            }
                        }
                    }

                    match ps.active.take().unwrap() {
                        ActivePartition::Hash {
                            task,
                            build_map,
                            mut probe_reader,
                            build_is_left,
                        } => {
                            let probe_row = probe_reader.next_row(
                                ctx.temp_storage,
                                &mut *ctx.disk_reader,
                                &mut *ctx.disk_writer,
                            )?;

                            match probe_row {
                                None => {
                                    self.cleanup_task(ctx, task)?;
                                    self.state = HashJoinState::Probing(ps);
                                }
                                Some(probe_row) => {
                                    let probe_keys: &[usize] = if build_is_left {
                                        &self.right_key_indices
                                    } else {
                                        &self.left_key_indices
                                    };
                                    let key = make_key(&probe_row, probe_keys);
                                    if let Some(build_rows) = build_map.get(&key) {
                                        for build_row in build_rows {
                                            let merged = if build_is_left {
                                                Row::merge(build_row, &probe_row)
                                            } else {
                                                Row::merge(&probe_row, build_row)
                                            };
                                            if eval_resolved(&merged, &self.resolved_extra)? {
                                                ps.output_buf.push(merged);
                                            }
                                        }
                                    }

                                    ps.active = Some(ActivePartition::Hash {
                                        task,
                                        build_map,
                                        probe_reader,
                                        build_is_left,
                                    });
                                    self.state = HashJoinState::Probing(ps);
                                }
                            }
                        }

                        ActivePartition::NestedLoop {
                            task,
                            inner_is_left,
                            inner_file,
                            mut outer_reader,
                            mut outer_batch,
                            mut inner_reader,
                        } => {
                            let maybe_inner = inner_reader.next_row(
                                ctx.temp_storage,
                                &mut *ctx.disk_reader,
                                &mut *ctx.disk_writer,
                            )?;

                            match maybe_inner {
                                Some(inner_row) => {
                                    for outer_row in outer_batch.iter().rev() {
                                        let merged = if inner_is_left {
                                            Row::merge(&inner_row, outer_row)
                                        } else {
                                            Row::merge(outer_row, &inner_row)
                                        };
                                        if eval_resolved(&merged, &self.resolved_extra)? {
                                            ps.output_buf.push(merged);
                                        }
                                    }

                                    ps.active = Some(ActivePartition::NestedLoop {
                                        task,
                                        inner_is_left,
                                        inner_file,
                                        outer_reader,
                                        outer_batch,
                                        inner_reader,
                                    });
                                    self.state = HashJoinState::Probing(ps);
                                }
                                None => {
                                    outer_batch = fill_temp_outer_batch(&mut outer_reader, ctx)?;
                                    if outer_batch.is_empty() {
                                        self.cleanup_task(ctx, task)?;
                                        self.state = HashJoinState::Probing(ps);
                                    } else {
                                        let inner_reader = TempRunReader::with_batch_pages(
                                            ctx.temp_storage,
                                            inner_file,
                                            HASH_JOIN_BUILD_READER_PAGES,
                                        )?;
                                        ps.active = Some(ActivePartition::NestedLoop {
                                            task,
                                            inner_is_left,
                                            inner_file,
                                            outer_reader,
                                            outer_batch,
                                            inner_reader,
                                        });
                                        self.state = HashJoinState::Probing(ps);
                                    }
                                }
                            }
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
    NeedOuterBatch { inner_file: TempFileId },
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
        let merged_schema = RowSchema::merge(&left_schema, &right_schema);
        let resolved = resolve_predicates(&merged_schema, &predicates)?;

        let inner_is_left = match (left_rows_hint, right_rows_hint) {
            (Some(l), Some(r)) => l <= r,
            _ => true,
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
        let block_size = ctx.temp_storage.block_size().max(1);
        let batch_pages = (ctx.sort_run_bytes / block_size).max(1).min(64);
        let mut writer = TempRunWriter::with_batch_pages(ctx.temp_storage, batch_pages)?;
        let mut inner = self.inner.take().expect("inner already consumed");
        loop {
            let row = match inner.next(ctx)? {
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
                            self.state = BlockNestedLoopState::NeedOuterBatch { inner_file };
                        }
                        Some(inner_row) => {
                            for outer_row in outer_batch.iter().rev() {
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
