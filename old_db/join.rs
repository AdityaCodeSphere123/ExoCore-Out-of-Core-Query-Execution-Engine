use anyhow::{anyhow, Result};
use common::query::{ComparisionOperator, ComparisionValue, Predicate, SortSpec};
use common::Data;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};

type FastHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;

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

use crate::filter::{eval_resolved, eval_resolved_two_parts, resolve_predicates, ResolvedPredicate};
use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};
use crate::sort;
use crate::temp_storage::{TempFileId, TempRunReader, TempRunWriter};

// ── join key ─────────────────────────────────────────────────────────────────

/// A row's join-key values, comparable and hashable.
#[derive(Eq, PartialEq, Clone)]
enum JoinKey {
    One(KeyField),
    Many(Vec<KeyField>),
}

#[derive(Eq, PartialEq, Clone)]
enum KeyField {
    I32(i32),
    I64(i64),
    F32(u32), // stored as bits so that bit-equal floats hash equally
    F64(u64),
    Str(String),
}

impl Hash for KeyField {
    /// Skip the enum discriminant and hash only the raw value bytes.
    ///
    /// For a schema-consistent join every key in the build map has the same
    /// type, so including the discriminant is pure overhead.  Different numeric
    /// widths (I32 vs I64) still produce different hashes because they feed a
    /// different number of bytes into FNV.  Collisions between incompatible
    /// types are possible but harmless (HashMap still uses `Eq` to confirm
    /// matches).
    #[inline]
    fn hash<H: Hasher>(&self, h: &mut H) {
        match self {
            KeyField::I32(v) => h.write_i32(*v),
            KeyField::I64(v) => h.write_i64(*v),
            KeyField::F32(v) => h.write_u32(*v),
            KeyField::F64(v) => h.write_u64(*v),
            KeyField::Str(s) => s.hash(h),
        }
    }
}

impl Hash for JoinKey {
    /// Skip the JoinKey discriminant — `One` vs `Many` is implicit in the key
    /// count, and every probe uses the same variant as the build side.
    #[inline]
    fn hash<H: Hasher>(&self, h: &mut H) {
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

fn key_hash64(row: &Row, indices: &[usize]) -> u64 {
    // Use our FNV hasher instead of DefaultHasher (SipHash): FNV is faster for
    // the small fixed-width inputs typical of join keys.
    let mut h = FastHasher::default();
    for &i in indices {
        match &row.values()[i] {
            Data::Int32(v) => v.hash(&mut h),
            Data::Int64(v) => v.hash(&mut h),
            Data::Float32(v) => v.to_bits().hash(&mut h),
            Data::Float64(v) => v.to_bits().hash(&mut h),
            Data::String(s) => s.hash(&mut h),
        }
    }
    h.finish()
}

fn single_int_key_value(row: &Row, indices: &[usize]) -> Option<i64> {
    let [idx] = indices else {
        return None;
    };
    match &row.values()[*idx] {
        Data::Int32(v) => Some(*v as i64),
        Data::Int64(v) => Some(*v),
        _ => None,
    }
}

/// Compute partition number directly from row values.
/// Avoids allocating a JoinKey (Vec + String clones) during the partitioning
/// phase where we only need the hash, not a comparable key.
fn partition_hash(row: &Row, indices: &[usize], num_partitions: usize) -> usize {
    (key_hash64(row, indices) as usize) % num_partitions
}

const MIN_HASH_JOIN_PARTITIONS: usize = 4;
const MAX_HASH_JOIN_PARTITIONS: usize = 20;
const TARGET_PARTITION_PAGES: usize = 800;
const IN_MEMORY_HASH_BUILD_NUM: usize = 1;
const IN_MEMORY_HASH_BUILD_DEN: usize = 3;
const HYBRID_RESIDENT_NUM: usize = 1;
const HYBRID_RESIDENT_DEN: usize = 6;
const SORT_MERGE_SWITCH_MULTIPLIER: usize = 4;
const ESTIMATED_AVG_COL_BYTES: usize = 24;
const ESTIMATED_ROW_OVERHEAD_BYTES: usize = 32;
const PROBE_PRUNE_FILTER_BITS: usize = 65_536;
const PROBE_PRUNE_FILTER_WORDS: usize = PROBE_PRUNE_FILTER_BITS / 64;
const PROBE_PRUNE_FILTER_HASHES: usize = 3;
const EXACT_KEY_BITMAP_MAX_BITS: usize = 16_777_216;
const HASH_BUILD_ROW_SAFETY_OVERHEAD_BYTES: usize = 96;

#[derive(Clone)]
struct PartitionKeyBloom {
    words: Vec<u64>,
}

impl PartitionKeyBloom {
    fn new() -> Self {
        Self {
            words: vec![0u64; PROBE_PRUNE_FILTER_WORDS],
        }
    }

    fn add_hash(&mut self, hash: u64) {
        let mut x = hash;
        for _ in 0..PROBE_PRUNE_FILTER_HASHES {
            let bit = (x as usize) & (PROBE_PRUNE_FILTER_BITS - 1);
            self.words[bit >> 6] |= 1u64 << (bit & 63);
            x = x.rotate_left(17).wrapping_mul(0x9e3779b97f4a7c15);
        }
    }

    fn might_contain_hash(&self, hash: u64) -> bool {
        let mut x = hash;
        for _ in 0..PROBE_PRUNE_FILTER_HASHES {
            let bit = (x as usize) & (PROBE_PRUNE_FILTER_BITS - 1);
            if (self.words[bit >> 6] & (1u64 << (bit & 63))) == 0 {
                return false;
            }
            x = x.rotate_left(17).wrapping_mul(0x9e3779b97f4a7c15);
        }
        true
    }
}

#[derive(Clone, Copy, Default)]
struct ExactBitmapCandidate {
    supported: bool,
    seen_any: bool,
    min_value: i64,
    max_value: i64,
}

impl ExactBitmapCandidate {
    fn observe_row(&mut self, row: &Row, key_indices: &[usize]) {
        // Once we've seen at least one row and decided the bitmap is not
        // supported, no future row can change that; skip immediately.
        if self.seen_any && !self.supported {
            return;
        }
        let Some(v) = single_int_key_value(row, key_indices) else {
            // Mark seen_any so the guard fires on every subsequent call,
            // avoiding repeated calls to single_int_key_value for String keys.
            self.supported = false;
            self.seen_any = true;
            return;
        };
        if !self.seen_any {
            self.supported = true;
            self.seen_any = true;
            self.min_value = v;
            self.max_value = v;
            return;
        }
        if v < self.min_value {
            self.min_value = v;
        }
        if v > self.max_value {
            self.max_value = v;
        }
    }

    fn can_build_bitmap(&self) -> bool {
        if !self.supported || !self.seen_any {
            return false;
        }
        let span = (self.max_value as i128) - (self.min_value as i128) + 1;
        span > 0 && (span as usize) <= EXACT_KEY_BITMAP_MAX_BITS
    }
}

struct ExactIntBitmap {
    base: i64,
    bits: Vec<u64>,
    /// Precomputed `bits.len() * 64` so `add_value` / `contains` skip the
    /// multiply on every call (this is invoked for each probe row).
    num_bits: usize,
}

impl ExactIntBitmap {
    fn new(base: i64, max_value: i64) -> Self {
        let span = ((max_value as i128) - (base as i128) + 1) as usize;
        let words = (span + 63) / 64;
        Self {
            base,
            num_bits: words * 64,
            bits: vec![0u64; words],
        }
    }

    fn add_value(&mut self, value: i64) {
        if value < self.base {
            return;
        }
        let offset = (value - self.base) as usize;
        if offset >= self.num_bits {
            return;
        }
        self.bits[offset >> 6] |= 1u64 << (offset & 63);
    }

    fn contains(&self, value: i64) -> bool {
        if value < self.base {
            return false;
        }
        let offset = (value - self.base) as usize;
        if offset >= self.num_bits {
            return false;
        }
        (self.bits[offset >> 6] & (1u64 << (offset & 63))) != 0
    }
}

fn choose_num_partitions(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size();
    let target_bytes = TARGET_PARTITION_PAGES.saturating_mul(block_size).max(1);
    (ctx.sort_run_bytes / target_bytes).clamp(MIN_HASH_JOIN_PARTITIONS, MAX_HASH_JOIN_PARTITIONS)
}

/// Compute reader batch pages for a single sequential-scan reader.
/// `num_concurrent` is how many readers will be active simultaneously so the
/// budget can be shared. Larger batches mean fewer refill calls and lower
/// rotational-latency penalties from extent boundary crossings.
fn choose_reader_batch(ctx: &ExecContext, num_concurrent: usize) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    let per_reader = ctx.sort_run_bytes / num_concurrent.max(1);
    let pages = per_reader / block_size;
    pages.clamp(16, 128)
}

/// Compute writer batch pages from available memory and partition count.
/// Larger batches mean fewer, bigger contiguous extents and therefore fewer
/// seeks during probe.
fn choose_writer_batch(sort_run_bytes: usize, block_size: usize, num_partitions: usize) -> usize {
    let budget = sort_run_bytes / 4;
    let per_writer = budget / num_partitions.max(1);
    let batch_pages = per_writer / block_size.max(1);
    batch_pages.clamp(16, 256)
}

fn choose_in_memory_build_budget_bytes(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    (ctx.sort_run_bytes * IN_MEMORY_HASH_BUILD_NUM / IN_MEMORY_HASH_BUILD_DEN).max(block_size)
}

fn choose_hybrid_resident_budget_bytes(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    (ctx.sort_run_bytes * HYBRID_RESIDENT_NUM / HYBRID_RESIDENT_DEN).max(block_size)
}

fn estimated_row_width_bytes(schema: &RowSchema) -> usize {
    ESTIMATED_ROW_OVERHEAD_BYTES + schema.len() * ESTIMATED_AVG_COL_BYTES
}

fn estimated_relation_bytes(rows_hint: f64, schema: &RowSchema) -> f64 {
    rows_hint.max(1.0) * estimated_row_width_bytes(schema) as f64
}

fn estimate_hash_key_size_bytes(row: &Row, key_indices: &[usize]) -> usize {
    use std::mem::size_of;

    let mut total = size_of::<JoinKey>() + if key_indices.len() <= 1 { 0 } else { key_indices.len() * size_of::<KeyField>() + 16 };
    for &idx in key_indices {
        if let Data::String(s) = &row.values()[idx] {
            total += s.capacity() + 16;
        }
    }
    total
}

fn estimate_hash_build_row_bytes(row: &Row, key_indices: &[usize]) -> usize {
    row.estimate_heap_size()
        + estimate_hash_key_size_bytes(row, key_indices)
        + HASH_BUILD_ROW_SAFETY_OVERHEAD_BYTES
}

fn estimate_partition_filter_bytes(num_partitions: usize) -> usize {
    num_partitions
        .saturating_mul(PROBE_PRUNE_FILTER_WORDS)
        .saturating_mul(std::mem::size_of::<u64>())
}

fn estimate_exact_bitmap_bytes(bitmap: &ExactIntBitmap) -> usize {
    bitmap.bits.len().saturating_mul(std::mem::size_of::<u64>())
}

fn estimate_probe_bloom_bytes() -> usize {
    PROBE_PRUNE_FILTER_WORDS.saturating_mul(std::mem::size_of::<u64>())
}

fn hash_join_key64(key: &JoinKey) -> u64 {
    let mut h = FastHasher::default();
    key.hash(&mut h);
    h.finish()
}

fn build_probe_bloom_from_build_map(build_map: &FastHashMap<JoinKey, Vec<Row>>) -> PartitionKeyBloom {
    let mut bloom = PartitionKeyBloom::new();
    for key in build_map.keys() {
        bloom.add_hash(hash_join_key64(key));
    }
    bloom
}

fn build_in_memory_probe_filter(
    ctx: &mut ExecContext,
    build_map: &FastHashMap<JoinKey, Vec<Row>>,
    exact_bitmap_candidate: &ExactBitmapCandidate,
) -> Result<Option<ProbeSemijoinFilter>> {
    if build_map.is_empty() {
        return Ok(None);
    }

    if exact_bitmap_candidate.can_build_bitmap() {
        let bitmap = ExactIntBitmap::new(
            exact_bitmap_candidate.min_value,
            exact_bitmap_candidate.max_value,
        );
        let bitmap_bytes = estimate_exact_bitmap_bytes(&bitmap);
        if ctx.available_memory() >= bitmap_bytes {
            ctx.try_reserve_memory(bitmap_bytes)?;
            let mut bitmap = bitmap;
            populate_bitmap_from_build_map(build_map, &mut bitmap);
            return Ok(Some(ProbeSemijoinFilter::ExactInt {
                bitmap,
                reserved_bytes: bitmap_bytes,
            }));
        }
    }

    let bloom_bytes = estimate_probe_bloom_bytes();
    if ctx.available_memory() >= bloom_bytes {
        ctx.try_reserve_memory(bloom_bytes)?;
        let bloom = build_probe_bloom_from_build_map(build_map);
        return Ok(Some(ProbeSemijoinFilter::Bloom {
            bloom,
            reserved_bytes: bloom_bytes,
        }));
    }

    Ok(None)
}

fn probe_filter_might_match(
    filter: Option<&ProbeSemijoinFilter>,
    probe_row: &Row,
    probe_keys: &[usize],
) -> bool {
    match filter {
        None => true,
        Some(ProbeSemijoinFilter::ExactInt { bitmap, .. }) => single_int_key_value(probe_row, probe_keys)
            .map(|value| bitmap.contains(value))
            .unwrap_or(true),
        Some(ProbeSemijoinFilter::Bloom { bloom, .. }) => {
            let key_hash = key_hash64(probe_row, probe_keys);
            bloom.might_contain_hash(key_hash)
        }
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemijoinKeepSide {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinOutputMode {
    Full,
    KeepLeft,
    KeepRight,
}

/// Build a join operator without row-count hints. Uses the adaptive equi-join
/// path (in-memory hash → hybrid/grace hash, or sort-merge for large balanced
/// joins) when equi-predicates exist; otherwise falls back to block nested loop.
pub fn build_join<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
) -> Result<Box<dyn Operator + 'a>> {
    build_join_impl(left, right, predicates, None, None, JoinOutputMode::Full)
}

/// Same as `build_join` but accepts estimated row counts for both sides.
/// These hints help choose the build side and whether sort-merge is worth
/// trying for very large equi-joins.
pub fn build_join_hinted<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
    left_rows_hint: f64,
    right_rows_hint: f64,
) -> Result<Box<dyn Operator + 'a>> {
    build_join_impl(
        left,
        right,
        predicates,
        Some(left_rows_hint),
        Some(right_rows_hint),
        JoinOutputMode::Full,
    )
}

pub fn build_semijoin_hinted<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
    keep_side: SemijoinKeepSide,
    left_rows_hint: f64,
    right_rows_hint: f64,
) -> Result<Box<dyn Operator + 'a>> {
    let output_mode = match keep_side {
        SemijoinKeepSide::Left => JoinOutputMode::KeepLeft,
        SemijoinKeepSide::Right => JoinOutputMode::KeepRight,
    };
    build_join_impl(
        left,
        right,
        predicates,
        Some(left_rows_hint),
        Some(right_rows_hint),
        output_mode,
    )
}

fn build_join_impl<'a>(
    left: Box<dyn Operator + 'a>,
    right: Box<dyn Operator + 'a>,
    predicates: &[Predicate],
    left_rows_hint: Option<f64>,
    right_rows_hint: Option<f64>,
    output_mode: JoinOutputMode,
) -> Result<Box<dyn Operator + 'a>> {
    let left_schema = left.schema().clone();
    let right_schema = right.schema().clone();
    let (left_key_indices, right_key_indices, extra_predicates) =
        split_join_predicates(&left_schema, &right_schema, predicates);

    if !left_key_indices.is_empty() {
        Ok(Box::new(AdaptiveEquiJoinOperator::new(
            left,
            right,
            left_schema,
            right_schema,
            left_key_indices,
            right_key_indices,
            extra_predicates,
            left_rows_hint,
            right_rows_hint,
            output_mode,
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

// ── Adaptive equi-join ───────────────────────────────────────────────────────

pub struct AdaptiveEquiJoinOperator<'a> {
    left: Option<Box<dyn Operator + 'a>>,
    right: Option<Box<dyn Operator + 'a>>,
    left_schema: RowSchema,
    right_schema: RowSchema,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    resolved_extra: Vec<ResolvedPredicate>,
    merged_schema: RowSchema,
    output_schema: RowSchema,
    output_mode: JoinOutputMode,
    force_build_left: Option<bool>,
    left_rows_hint: Option<f64>,
    right_rows_hint: Option<f64>,
    state: AdaptiveEquiJoinState<'a>,
}

enum AdaptiveEquiJoinState<'a> {
    NotStarted,
    InMemory(InMemoryHashState<'a>),
    Probing(ProbingState),
    SortMerge(SortMergeState<'a>),
    Done,
}

struct PendingProbeMatches {
    probe_row: Row,
    match_key: JoinKey,
    next_match_idx: usize,
}

enum ProbeSemijoinFilter {
    ExactInt { bitmap: ExactIntBitmap, reserved_bytes: usize },
    Bloom { bloom: PartitionKeyBloom, reserved_bytes: usize },
}

impl ProbeSemijoinFilter {
    fn reserved_bytes(&self) -> usize {
        match self {
            Self::ExactInt { reserved_bytes, .. } => *reserved_bytes,
            Self::Bloom { reserved_bytes, .. } => *reserved_bytes,
        }
    }
}

struct InMemoryHashState<'a> {
    build_map: FastHashMap<JoinKey, Vec<Row>>,
    probe: Box<dyn Operator + 'a>,
    pending_match: Option<PendingProbeMatches>,
    probe_filter: Option<ProbeSemijoinFilter>,
    build_is_left: bool,
    reserved_bytes: usize,
}

struct ProbingState {
    left_parts: Vec<TempFileId>,
    right_parts: Vec<TempFileId>,
    resident_build_map: FastHashMap<JoinKey, Vec<Row>>,
    resident_build_reserved_bytes: usize,
    resident_probe_file: Option<TempFileId>,
    resident_build_spill_file: Option<TempFileId>,
    resident_phase_done: bool,
    current_part: usize,
    build_map: FastHashMap<JoinKey, Vec<Row>>,
    build_map_reserved_bytes: usize,
    probe_reader: Option<TempRunReader>,
    active_probe_file: Option<TempFileId>,
    pending_match: Option<PendingProbeMatches>,
    build_is_left: bool,
    processing_resident: bool,
}

struct SortMergeState<'a> {
    left_sorted: Box<dyn Operator + 'a>,
    right_sorted: Box<dyn Operator + 'a>,
    current_left: Option<Row>,
    current_right: Option<Row>,
    output_buf: Vec<Row>,
}

impl<'a> AdaptiveEquiJoinOperator<'a> {
    fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        left_schema: RowSchema,
        right_schema: RowSchema,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        extra_predicates: Vec<Predicate>,
        left_rows_hint: Option<f64>,
        right_rows_hint: Option<f64>,
        output_mode: JoinOutputMode,
    ) -> Result<Self> {
        let merged_schema = RowSchema::merge(&left_schema, &right_schema);
        let resolved_extra = resolve_predicates(&merged_schema, &extra_predicates)?;
        let output_schema = match output_mode {
            JoinOutputMode::Full => merged_schema.clone(),
            JoinOutputMode::KeepLeft => left_schema.clone(),
            JoinOutputMode::KeepRight => right_schema.clone(),
        };
        let force_build_left = match output_mode {
            JoinOutputMode::Full => None,
            JoinOutputMode::KeepLeft => Some(false),
            JoinOutputMode::KeepRight => Some(true),
        };
        Ok(Self {
            left: Some(left),
            right: Some(right),
            left_schema,
            right_schema,
            left_key_indices,
            right_key_indices,
            resolved_extra,
            merged_schema,
            output_schema,
            output_mode,
            force_build_left,
            left_rows_hint,
            right_rows_hint,
            state: AdaptiveEquiJoinState::NotStarted,
        })
    }

    fn prefer_build_left(&self) -> bool {
        if let Some(force) = self.force_build_left {
            return force;
        }
        match (self.left_rows_hint, self.right_rows_hint) {
            (Some(l), Some(r)) => l <= r,
            _ => true,
        }
    }

    fn should_use_sort_merge(&self, ctx: &ExecContext) -> bool {
        if self.output_mode != JoinOutputMode::Full {
            return false;
        }

        let (Some(left_rows), Some(right_rows)) = (self.left_rows_hint, self.right_rows_hint) else {
            return false;
        };

        let left_bytes = estimated_relation_bytes(left_rows, &self.left_schema);
        let right_bytes = estimated_relation_bytes(right_rows, &self.right_schema);
        let threshold = (choose_in_memory_build_budget_bytes(ctx) * SORT_MERGE_SWITCH_MULTIPLIER) as f64;

        left_bytes >= threshold && right_bytes >= threshold
    }

    fn start_sort_merge(&mut self, ctx: &mut ExecContext) -> Result<SortMergeState<'a>> {
        let left = self.left.take().expect("left already consumed");
        let right = self.right.take().expect("right already consumed");

        let left_sort_specs = build_sort_specs(&self.left_schema, &self.left_key_indices);
        let right_sort_specs = build_sort_specs(&self.right_schema, &self.right_key_indices);

        let mut left_sorted: Box<dyn Operator + 'a> =
            Box::new(sort::SortOperator::new(left, &left_sort_specs)?);
        let mut right_sorted: Box<dyn Operator + 'a> =
            Box::new(sort::SortOperator::new(right, &right_sort_specs)?);

        let current_left = left_sorted.next(ctx)?;
        let current_right = right_sorted.next(ctx)?;

        Ok(SortMergeState {
            left_sorted,
            right_sorted,
            current_left,
            current_right,
            output_buf: Vec::new(),
        })
    }

    fn start_hash_family(&mut self, ctx: &mut ExecContext) -> Result<AdaptiveEquiJoinState<'a>> {
        let left = self.left.take().expect("left already consumed");
        let right = self.right.take().expect("right already consumed");

        let build_is_left = self.prefer_build_left();
        let (mut build, probe) = if build_is_left { (left, right) } else { (right, left) };
        let build_budget = choose_in_memory_build_budget_bytes(ctx);
        let build_keys: &[usize] = if build_is_left {
            &self.left_key_indices
        } else {
            &self.right_key_indices
        };

        let mut prefetched_build_rows = Vec::new();
        let mut build_bytes = 0usize;
        let mut exact_bitmap_candidate = ExactBitmapCandidate::default();

        loop {
            let maybe_row = build.next(ctx)?;
            let row = match maybe_row {
                Some(r) => r,
                None => {
                    let mut build_map: FastHashMap<JoinKey, Vec<Row>> = FastHashMap::default();
                    for row in prefetched_build_rows.drain(..) {
                        insert_build_row(&mut build_map, row, build_keys);
                    }
                    let probe_filter = build_in_memory_probe_filter(
                        ctx,
                        &build_map,
                        &exact_bitmap_candidate,
                    )?;
                    return Ok(AdaptiveEquiJoinState::InMemory(InMemoryHashState {
                        build_map,
                        probe,
                        pending_match: None,
                        probe_filter,
                        build_is_left,
                        reserved_bytes: build_bytes,
                    }));
                }
            };

            let row_bytes = estimate_hash_build_row_bytes(&row, build_keys);
            if build_bytes + row_bytes <= build_budget && ctx.available_memory() >= row_bytes {
                exact_bitmap_candidate.observe_row(&row, build_keys);
                ctx.try_reserve_memory(row_bytes)?;
                build_bytes += row_bytes;
                prefetched_build_rows.push(row);
                continue;
            }

            let probing = self.partition_hybrid_or_grace(
                ctx,
                build_is_left,
                prefetched_build_rows,
                build_bytes,
                row,
                build,
                probe,
            )?;
            return Ok(AdaptiveEquiJoinState::Probing(probing));
        }
    }

    fn partition_hybrid_or_grace(
        &mut self,
        ctx: &mut ExecContext,
        build_is_left: bool,
        prefetched_build_rows: Vec<Row>,
        prefetched_reserved_bytes: usize,
        first_overflow_row: Row,
        mut build: Box<dyn Operator + 'a>,
        mut probe: Box<dyn Operator + 'a>,
    ) -> Result<ProbingState> {
        ctx.release_memory(prefetched_reserved_bytes);

        let num_partitions = choose_num_partitions(ctx);
        let writer_batch = choose_writer_batch(
            ctx.sort_run_bytes,
            ctx.temp_storage.block_size(),
            num_partitions,
        );
        let resident_part = 0usize;
        let resident_budget = choose_hybrid_resident_budget_bytes(ctx);

        let build_keys: &[usize] = if build_is_left {
            &self.left_key_indices
        } else {
            &self.right_key_indices
        };
        let probe_keys: &[usize] = if build_is_left {
            &self.right_key_indices
        } else {
            &self.left_key_indices
        };

        let mut build_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();
        let mut probe_writers: Vec<Option<TempRunWriter>> =
            (0..num_partitions).map(|_| None).collect();

        let mut resident_build_map: FastHashMap<JoinKey, Vec<Row>> = FastHashMap::default();
        let mut resident_bytes = 0usize;
        let mut build_key_filters: Vec<PartitionKeyBloom> =
            (0..num_partitions).map(|_| PartitionKeyBloom::new()).collect();
        let mut filter_reserved_bytes = estimate_partition_filter_bytes(num_partitions);
        ctx.try_reserve_memory(filter_reserved_bytes)?;
        let mut exact_bitmap_candidate = ExactBitmapCandidate::default();

        for row in prefetched_build_rows {
            process_build_row_for_hybrid(
                row,
                build_keys,
                num_partitions,
                resident_part,
                resident_budget,
                &mut resident_build_map,
                &mut resident_bytes,
                &mut build_writers,
                &mut build_key_filters,
                &mut exact_bitmap_candidate,
                writer_batch,
                ctx,
            )?;
        }
        process_build_row_for_hybrid(
            first_overflow_row,
            build_keys,
            num_partitions,
            resident_part,
            resident_budget,
            &mut resident_build_map,
            &mut resident_bytes,
            &mut build_writers,
            &mut build_key_filters,
            &mut exact_bitmap_candidate,
            writer_batch,
            ctx,
        )?;
        while let Some(row) = build.next(ctx)? {
            process_build_row_for_hybrid(
                row,
                build_keys,
                num_partitions,
                resident_part,
                resident_budget,
                &mut resident_build_map,
                &mut resident_bytes,
                &mut build_writers,
                &mut build_key_filters,
                &mut exact_bitmap_candidate,
                writer_batch,
                ctx,
            )?;
        }

        let mut build_part_files: Vec<Option<TempFileId>> = Vec::with_capacity(num_partitions);
        for writer_opt in build_writers.into_iter() {
            let file_opt = match writer_opt {
                Some(writer) => Some(writer.finish(
                    ctx.temp_storage,
                    &mut *ctx.disk_reader,
                    &mut *ctx.disk_writer,
                )?),
                None => None,
            };
            build_part_files.push(file_opt);
        }

        let mut exact_probe_bitmap = if exact_bitmap_candidate.can_build_bitmap() {
            let bitmap = ExactIntBitmap::new(
                exact_bitmap_candidate.min_value,
                exact_bitmap_candidate.max_value,
            );
            let bitmap_bytes = estimate_exact_bitmap_bytes(&bitmap);
            if ctx.available_memory() >= bitmap_bytes {
                ctx.try_reserve_memory(bitmap_bytes)?;
                filter_reserved_bytes += bitmap_bytes;
                Some(bitmap)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(bitmap) = exact_probe_bitmap.as_mut() {
            populate_bitmap_from_build_map(&resident_build_map, bitmap);
            for file_opt in build_part_files.iter() {
                if let Some(file_id) = file_opt {
                    populate_bitmap_from_build_file(ctx, *file_id, build_keys, bitmap)?;
                }
            }
        }

        let resident_build_spilled = build_part_files[resident_part].is_some();
        let mut resident_probe_writer: Option<TempRunWriter> = None;

        while let Some(row) = probe.next(ctx)? {
            let key_hash = key_hash64(&row, probe_keys);
            let part = (key_hash as usize) % num_partitions;
            let keep_probe_row = if let Some(bitmap) = exact_probe_bitmap.as_ref() {
                single_int_key_value(&row, probe_keys)
                    .map(|value| bitmap.contains(value))
                    .unwrap_or_else(|| build_key_filters[part].might_contain_hash(key_hash))
            } else {
                build_key_filters[part].might_contain_hash(key_hash)
            };
            if !keep_probe_row {
                continue;
            }
            if part == resident_part {
                if resident_build_spilled || !resident_build_map.is_empty() {
                    if resident_probe_writer.is_none() {
                        resident_probe_writer = Some(TempRunWriter::with_batch_pages(
                            ctx.temp_storage,
                            writer_batch,
                        )?);
                    }
                    resident_probe_writer
                        .as_mut()
                        .expect("resident probe writer must exist")
                        .append_row(
                            &row,
                            ctx.temp_storage,
                            &mut *ctx.disk_reader,
                            &mut *ctx.disk_writer,
                        )?;
                }
            } else {
                append_row_to_partition_writer(
                    &mut probe_writers,
                    part,
                    &row,
                    writer_batch,
                    ctx,
                )?;
            }
        }

        let resident_build_spill_file = build_part_files[resident_part].take();
        let resident_probe_file = match resident_probe_writer {
            Some(writer) => Some(writer.finish(
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?),
            None => None,
        };
        probe_writers[resident_part] = None;

        let (left_parts, right_parts) = finalize_partition_pairs(
            ctx,
            build_is_left,
            build_part_files,
            probe_writers,
        )?;

        let resident_phase_done = resident_probe_file.is_none();
        let resident_build_spill_file = if resident_phase_done {
            if let Some(file_id) = resident_build_spill_file {
                ctx.temp_storage.delete_temp_file(file_id)?;
            }
            ctx.release_memory(resident_bytes);
            resident_bytes = 0;
            resident_build_map.clear();
            None
        } else {
            resident_build_spill_file
        };

        ctx.release_memory(filter_reserved_bytes);

        Ok(ProbingState {
            left_parts,
            right_parts,
            resident_build_map,
            resident_build_reserved_bytes: resident_bytes,
            resident_probe_file,
            resident_build_spill_file,
            resident_phase_done,
            current_part: 0,
            build_map: FastHashMap::default(),
            build_map_reserved_bytes: 0,
            probe_reader: None,
            active_probe_file: None,
            pending_match: None,
            build_is_left,
            processing_resident: false,
        })
    }


}

impl<'a> Operator for AdaptiveEquiJoinOperator<'a> {
    fn schema(&self) -> &RowSchema {
        &self.output_schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        loop {
            match std::mem::replace(&mut self.state, AdaptiveEquiJoinState::Done) {
                AdaptiveEquiJoinState::NotStarted => {
                    self.state = if self.should_use_sort_merge(ctx) {
                        AdaptiveEquiJoinState::SortMerge(self.start_sort_merge(ctx)?)
                    } else {
                        self.start_hash_family(ctx)?
                    };
                }

                AdaptiveEquiJoinState::Done => return Ok(None),

                AdaptiveEquiJoinState::InMemory(mut ims) => {
                    if self.output_mode == JoinOutputMode::Full {
                        if let Some(row) = emit_pending_match(
                            &ims.build_map,
                            ims.pending_match.as_mut(),
                            ims.build_is_left,
                            &self.resolved_extra,
                        )? {
                            self.state = AdaptiveEquiJoinState::InMemory(ims);
                            return Ok(Some(row));
                        }
                        ims.pending_match = None;
                    }

                    let maybe_probe = ims.probe.next(ctx)?;
                    let probe_row = match maybe_probe {
                        Some(r) => r,
                        None => {
                            let filter_reserved = ims
                                .probe_filter
                                .as_ref()
                                .map(ProbeSemijoinFilter::reserved_bytes)
                                .unwrap_or(0);
                            ctx.release_memory(ims.reserved_bytes + filter_reserved);
                            return Ok(None);
                        },
                    };

                    let probe_keys: &[usize] = if ims.build_is_left {
                        &self.right_key_indices
                    } else {
                        &self.left_key_indices
                    };
                    if !probe_filter_might_match(ims.probe_filter.as_ref(), &probe_row, probe_keys) {
                        self.state = AdaptiveEquiJoinState::InMemory(ims);
                        continue;
                    }

                    if self.output_mode == JoinOutputMode::Full {
                        let key = make_key(&probe_row, probe_keys);
                        // Single get() — eliminates contains_key() + the two
                        // separate get() calls inside emit_pending_match that
                        // would otherwise be needed (3 hash ops → 1 for 1:1 joins).
                        if let Some(build_rows) = ims.build_map.get(&key) {
                            let build_is_left = ims.build_is_left;
                            let mut idx = 0usize;
                            while idx < build_rows.len() {
                                let build_row = &build_rows[idx];
                                idx += 1;
                                let merged = if build_is_left {
                                    Row::merge(build_row, &probe_row)
                                } else {
                                    Row::merge(&probe_row, build_row)
                                };
                                if eval_resolved(&merged, &self.resolved_extra)? {
                                    if idx < build_rows.len() {
                                        // Multi-match: store pending for the rest
                                        ims.pending_match = Some(PendingProbeMatches {
                                            probe_row,
                                            match_key: key,
                                            next_match_idx: idx,
                                        });
                                    }
                                    self.state = AdaptiveEquiJoinState::InMemory(ims);
                                    return Ok(Some(merged));
                                }
                            }
                        }
                    } else if probe_row_has_qualifying_match(
                        &ims.build_map,
                        &probe_row,
                        probe_keys,
                        ims.build_is_left,
                        &self.resolved_extra,
                    )? {
                        self.state = AdaptiveEquiJoinState::InMemory(ims);
                        return Ok(Some(probe_row));
                    }

                    self.state = AdaptiveEquiJoinState::InMemory(ims);
                }

                AdaptiveEquiJoinState::Probing(mut ps) => {
                    if self.output_mode == JoinOutputMode::Full {
                        if let Some(row) = emit_pending_match(
                            &ps.build_map,
                            ps.pending_match.as_mut(),
                            ps.build_is_left,
                            &self.resolved_extra,
                        )? {
                            self.state = AdaptiveEquiJoinState::Probing(ps);
                            return Ok(Some(row));
                        }
                        ps.pending_match = None;
                    }

                    if ps.probe_reader.is_none() {
                        if !ps.resident_phase_done {
                            ps.processing_resident = true;
                            ps.build_map = std::mem::take(&mut ps.resident_build_map);
                            ps.build_map_reserved_bytes = ps.resident_build_reserved_bytes;
                            ps.resident_build_reserved_bytes = 0;

                            if let Some(build_file) = ps.resident_build_spill_file.take() {
                                let build_keys: &[usize] = if ps.build_is_left {
                                    &self.left_key_indices
                                } else {
                                    &self.right_key_indices
                                };
                                load_build_file_into_map(ctx, build_file, build_keys, &mut ps.build_map, &mut ps.build_map_reserved_bytes)?;
                                ctx.temp_storage.delete_temp_file(build_file)?;
                            }

                            let probe_file = match ps.resident_probe_file.take() {
                                Some(file) => file,
                                None => {
                                    ps.resident_phase_done = true;
                                    ps.processing_resident = false;
                                    ctx.release_memory(ps.build_map_reserved_bytes);
                                    ps.build_map_reserved_bytes = 0;
                                    ps.build_map.clear();
                                    self.state = AdaptiveEquiJoinState::Probing(ps);
                                    continue;
                                }
                            };

                            let probe_reader_pages = choose_reader_batch(ctx, 1);
                            ps.probe_reader = Some(TempRunReader::with_batch_pages(
                                ctx.temp_storage,
                                probe_file,
                                probe_reader_pages,
                            )?);
                            ps.active_probe_file = Some(probe_file);
                        } else {
                            if ps.current_part >= ps.left_parts.len() {
                                ctx.release_memory(ps.build_map_reserved_bytes);
                                ctx.release_memory(ps.resident_build_reserved_bytes);
                                return Ok(None);
                            }

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

                            ps.build_map.clear();
                            load_build_file_into_map(ctx, build_file, build_keys, &mut ps.build_map, &mut ps.build_map_reserved_bytes)?;
                            ctx.temp_storage.delete_temp_file(build_file)?;

                            let probe_reader_pages = choose_reader_batch(ctx, 1);
                            ps.probe_reader = Some(TempRunReader::with_batch_pages(
                                ctx.temp_storage,
                                probe_file,
                                probe_reader_pages,
                            )?);
                            ps.active_probe_file = Some(probe_file);
                        }
                    }

                    let probe_row = {
                        let reader = ps.probe_reader.as_mut().expect("probe reader must exist");
                        reader.next_row(
                            ctx.temp_storage,
                            &mut *ctx.disk_reader,
                            &mut *ctx.disk_writer,
                        )?
                    };

                    match probe_row {
                        None => {
                            if let Some(file_id) = ps.active_probe_file.take() {
                                ctx.temp_storage.delete_temp_file(file_id)?;
                            }
                            ps.probe_reader = None;
                            ctx.release_memory(ps.build_map_reserved_bytes);
                            ps.build_map_reserved_bytes = 0;
                            ps.build_map.clear();
                            ps.pending_match = None;

                            if ps.processing_resident {
                                ps.resident_phase_done = true;
                                ps.processing_resident = false;
                            } else {
                                ps.current_part += 1;
                            }

                            self.state = AdaptiveEquiJoinState::Probing(ps);
                        }
                        Some(probe_row) => {
                            let probe_keys: &[usize] = if ps.build_is_left {
                                &self.right_key_indices
                            } else {
                                &self.left_key_indices
                            };
                            if self.output_mode == JoinOutputMode::Full {
                                let key = make_key(&probe_row, probe_keys);
                                if let Some(build_rows) = ps.build_map.get(&key) {
                                    let build_is_left = ps.build_is_left;
                                    let mut idx = 0usize;
                                    while idx < build_rows.len() {
                                        let build_row = &build_rows[idx];
                                        idx += 1;
                                        let merged = if build_is_left {
                                            Row::merge(build_row, &probe_row)
                                        } else {
                                            Row::merge(&probe_row, build_row)
                                        };
                                        if eval_resolved(&merged, &self.resolved_extra)? {
                                            if idx < build_rows.len() {
                                                ps.pending_match = Some(PendingProbeMatches {
                                                    probe_row,
                                                    match_key: key,
                                                    next_match_idx: idx,
                                                });
                                            }
                                            self.state = AdaptiveEquiJoinState::Probing(ps);
                                            return Ok(Some(merged));
                                        }
                                    }
                                }
                            } else if probe_row_has_qualifying_match(
                                &ps.build_map,
                                &probe_row,
                                probe_keys,
                                ps.build_is_left,
                                &self.resolved_extra,
                            )? {
                                self.state = AdaptiveEquiJoinState::Probing(ps);
                                return Ok(Some(probe_row));
                            }
                            self.state = AdaptiveEquiJoinState::Probing(ps);
                        }
                    }
                }

                AdaptiveEquiJoinState::SortMerge(mut sm) => {
                    if let Some(row) = sm.output_buf.pop() {
                        self.state = AdaptiveEquiJoinState::SortMerge(sm);
                        return Ok(Some(row));
                    }

                    loop {
                        let cmp = match (&sm.current_left, &sm.current_right) {
                            (Some(left_row), Some(right_row)) => compare_join_keys(
                                left_row,
                                right_row,
                                &self.left_key_indices,
                                &self.right_key_indices,
                            ),
                            _ => return Ok(None),
                        };

                        match cmp {
                            Ordering::Less => {
                                sm.current_left = sm.left_sorted.next(ctx)?;
                            }
                            Ordering::Greater => {
                                sm.current_right = sm.right_sorted.next(ctx)?;
                            }
                            Ordering::Equal => {
                                let left_group_key = sm
                                    .current_left
                                    .as_ref()
                                    .expect("left row must exist")
                                    .clone();
                                let right_group_key = sm
                                    .current_right
                                    .as_ref()
                                    .expect("right row must exist")
                                    .clone();

                                let mut left_group =
                                    vec![sm.current_left.take().expect("left row must exist")];
                                let mut right_group =
                                    vec![sm.current_right.take().expect("right row must exist")];

                                loop {
                                    match sm.left_sorted.next(ctx)? {
                                        Some(next_left)
                                            if compare_same_side_keys(
                                                &next_left,
                                                &left_group_key,
                                                &self.left_key_indices,
                                            ) == Ordering::Equal =>
                                        {
                                            left_group.push(next_left);
                                        }
                                        Some(next_left) => {
                                            sm.current_left = Some(next_left);
                                            break;
                                        }
                                        None => {
                                            sm.current_left = None;
                                            break;
                                        }
                                    }
                                }

                                loop {
                                    match sm.right_sorted.next(ctx)? {
                                        Some(next_right)
                                            if compare_same_side_keys(
                                                &next_right,
                                                &right_group_key,
                                                &self.right_key_indices,
                                            ) == Ordering::Equal =>
                                        {
                                            right_group.push(next_right);
                                        }
                                        Some(next_right) => {
                                            sm.current_right = Some(next_right);
                                            break;
                                        }
                                        None => {
                                            sm.current_right = None;
                                            break;
                                        }
                                    }
                                }

                                for left_match in left_group.iter().rev() {
                                    for right_match in right_group.iter().rev() {
                                        let merged = Row::merge(left_match, right_match);
                                        if eval_resolved(&merged, &self.resolved_extra)? {
                                            sm.output_buf.push(merged);
                                        }
                                    }
                                }

                                if let Some(row) = sm.output_buf.pop() {
                                    self.state = AdaptiveEquiJoinState::SortMerge(sm);
                                    return Ok(Some(row));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn build_sort_specs(schema: &RowSchema, key_indices: &[usize]) -> Vec<SortSpec> {
    key_indices
        .iter()
        .map(|&idx| SortSpec {
            column_name: schema.column_names()[idx].clone(),
            ascending: true,
        })
        .collect()
}

/// Infallible comparison of one Data value — mirrors `compare_data_ord` in
/// sort.rs.  Join-key types are validated at operator construction time so a
/// type mismatch is a programming error; returning Equal keeps the merge safe.
#[inline]
fn compare_data_join(left: &Data, right: &Data) -> Ordering {
    match (left, right) {
        (Data::Int32(a), Data::Int32(b)) => a.cmp(b),
        (Data::Int64(a), Data::Int64(b)) => a.cmp(b),
        (Data::Float32(a), Data::Float32(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Data::Float64(a), Data::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Data::String(a), Data::String(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// Compare two rows on their respective join-key columns.  Infallible — uses
/// direct slice indexing since key indices are validated at construction time.
fn compare_join_keys(
    left: &Row,
    right: &Row,
    left_indices: &[usize],
    right_indices: &[usize],
) -> Ordering {
    let left_vals = left.values();
    let right_vals = right.values();
    for (&li, &ri) in left_indices.iter().zip(right_indices.iter()) {
        let ord = compare_data_join(&left_vals[li], &right_vals[ri]);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Compare a row against a reference row on the same-side key columns.
fn compare_same_side_keys(row: &Row, key_row: &Row, indices: &[usize]) -> Ordering {
    let row_vals = row.values();
    let key_vals = key_row.values();
    for &idx in indices {
        let ord = compare_data_join(&row_vals[idx], &key_vals[idx]);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn insert_build_row(build_map: &mut FastHashMap<JoinKey, Vec<Row>>, row: Row, key_indices: &[usize]) {
    let key = make_key(&row, key_indices);
    build_map.entry(key).or_default().push(row);
}

fn emit_pending_match(
    build_map: &FastHashMap<JoinKey, Vec<Row>>,
    pending: Option<&mut PendingProbeMatches>,
    build_is_left: bool,
    resolved_extra: &[ResolvedPredicate],
) -> Result<Option<Row>> {
    let Some(pending) = pending else {
        return Ok(None);
    };

    if let Some(build_rows) = build_map.get(&pending.match_key) {
        while pending.next_match_idx < build_rows.len() {
            let build_row = &build_rows[pending.next_match_idx];
            pending.next_match_idx += 1;

            let merged = if build_is_left {
                Row::merge(build_row, &pending.probe_row)
            } else {
                Row::merge(&pending.probe_row, build_row)
            };

            if eval_resolved(&merged, resolved_extra)? {
                return Ok(Some(merged));
            }
        }
    }

    Ok(None)
}


fn probe_row_has_qualifying_match(
    build_map: &FastHashMap<JoinKey, Vec<Row>>,
    probe_row: &Row,
    probe_keys: &[usize],
    build_is_left: bool,
    resolved_extra: &[ResolvedPredicate],
) -> Result<bool> {
    let key = make_key(probe_row, probe_keys);
    let Some(build_rows) = build_map.get(&key) else {
        return Ok(false);
    };

    if resolved_extra.is_empty() {
        return Ok(!build_rows.is_empty());
    }

    for build_row in build_rows {
        let merged = if build_is_left {
            Row::merge(build_row, probe_row)
        } else {
            Row::merge(probe_row, build_row)
        };
        if eval_resolved(&merged, resolved_extra)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn populate_bitmap_from_build_map(
    build_map: &FastHashMap<JoinKey, Vec<Row>>,
    bitmap: &mut ExactIntBitmap,
) {
    for key in build_map.keys() {
        if let JoinKey::One(KeyField::I32(v)) = key {
            bitmap.add_value(*v as i64);
        } else if let JoinKey::One(KeyField::I64(v)) = key {
            bitmap.add_value(*v);
        }
    }
}

fn populate_bitmap_from_build_file(
    ctx: &mut ExecContext,
    build_file: TempFileId,
    build_keys: &[usize],
    bitmap: &mut ExactIntBitmap,
) -> Result<()> {
    let build_reader_pages = choose_reader_batch(ctx, 1);
    let mut build_reader = TempRunReader::with_batch_pages(
        ctx.temp_storage,
        build_file,
        build_reader_pages,
    )?;

    while let Some(row) = build_reader.next_row(
        ctx.temp_storage,
        &mut *ctx.disk_reader,
        &mut *ctx.disk_writer,
    )? {
        if let Some(v) = single_int_key_value(&row, build_keys) {
            bitmap.add_value(v);
        }
    }

    Ok(())
}

fn load_build_file_into_map(
    ctx: &mut ExecContext,
    build_file: TempFileId,
    build_keys: &[usize],
    build_map: &mut FastHashMap<JoinKey, Vec<Row>>,
    reserved_bytes: &mut usize,
) -> Result<()> {
    let build_reader_pages = choose_reader_batch(ctx, 1);
    let mut build_reader = TempRunReader::with_batch_pages(
        ctx.temp_storage,
        build_file,
        build_reader_pages,
    )?;

    while let Some(row) = build_reader.next_row(
        ctx.temp_storage,
        &mut *ctx.disk_reader,
        &mut *ctx.disk_writer,
    )? {
        let row_bytes = estimate_hash_build_row_bytes(&row, build_keys);
        ctx.try_reserve_memory(row_bytes)?;
        *reserved_bytes += row_bytes;
        insert_build_row(build_map, row, build_keys);
    }

    Ok(())
}

fn process_build_row_for_hybrid(
    row: Row,
    build_keys: &[usize],
    num_partitions: usize,
    resident_part: usize,
    resident_budget: usize,
    resident_build_map: &mut FastHashMap<JoinKey, Vec<Row>>,
    resident_bytes: &mut usize,
    build_writers: &mut [Option<TempRunWriter>],
    build_key_filters: &mut [PartitionKeyBloom],
    exact_bitmap_candidate: &mut ExactBitmapCandidate,
    writer_batch: usize,
    ctx: &mut ExecContext,
) -> Result<()> {
    exact_bitmap_candidate.observe_row(&row, build_keys);
    let key_hash = key_hash64(&row, build_keys);
    let part = (key_hash as usize) % num_partitions;
    build_key_filters[part].add_hash(key_hash);
    let row_bytes = estimate_hash_build_row_bytes(&row, build_keys);
    if part == resident_part
        && *resident_bytes + row_bytes <= resident_budget
        && ctx.available_memory() >= row_bytes
    {
        ctx.try_reserve_memory(row_bytes)?;
        *resident_bytes += row_bytes;
        insert_build_row(resident_build_map, row, build_keys);
    } else {
        append_row_to_partition_writer(build_writers, part, &row, writer_batch, ctx)?;
    }
    Ok(())
}

fn append_row_to_partition_writer(
    writers: &mut [Option<TempRunWriter>],
    part: usize,
    row: &Row,
    writer_batch: usize,
    ctx: &mut ExecContext,
) -> Result<()> {
    if writers[part].is_none() {
        writers[part] = Some(TempRunWriter::with_batch_pages(
            ctx.temp_storage,
            writer_batch,
        )?);
    }
    writers[part]
        .as_mut()
        .expect("writer must exist")
        .append_row(
            row,
            ctx.temp_storage,
            &mut *ctx.disk_reader,
            &mut *ctx.disk_writer,
        )?;
    Ok(())
}

fn finalize_partition_pairs(
    ctx: &mut ExecContext,
    build_is_left: bool,
    mut build_files: Vec<Option<TempFileId>>,
    mut probe_writers: Vec<Option<TempRunWriter>>,
) -> Result<(Vec<TempFileId>, Vec<TempFileId>)> {
    let mut left_parts = Vec::new();
    let mut right_parts = Vec::new();

    for part in 0..build_files.len() {
        let probe_file = match probe_writers[part].take() {
            Some(probe_writer) => Some(probe_writer.finish(
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?),
            None => None,
        };

        match (build_files[part].take(), probe_file) {
            (Some(build_file), Some(probe_file)) => {
                if build_is_left {
                    left_parts.push(build_file);
                    right_parts.push(probe_file);
                } else {
                    left_parts.push(probe_file);
                    right_parts.push(build_file);
                }
            }
            (Some(build_file), None) => {
                ctx.temp_storage.delete_temp_file(build_file)?;
            }
            (None, Some(probe_file)) => {
                ctx.temp_storage.delete_temp_file(probe_file)?;
            }
            (None, None) => {}
        }
    }

    Ok((left_parts, right_parts))
}

// ── Block Nested-Loop Join (fallback for non-equi conditions) ───────────────

const MAX_BNLJ_OUTER_BATCH_PAGES: usize = 512;

fn choose_bnlj_outer_batch_bytes(ctx: &ExecContext) -> usize {
    let block_size = ctx.temp_storage.block_size().max(1);
    let total_pages = (ctx.sort_run_bytes / block_size).max(1);
    let batch_pages = (total_pages / 4).max(1).min(MAX_BNLJ_OUTER_BATCH_PAGES);
    batch_pages * block_size
}

/// Out-of-core block nested loop join used when no equi-join predicate exists.
///
/// Strategy:
///   1. Identify the "inner" side (smaller, spilled once to temp storage) and
///      the "outer" side (larger, read in memory-sized batches). When row-count
///      hints are provided, we spill whichever side has fewer rows; otherwise we
///      default to spilling left. Spilling the smaller side minimises the
///      number of disk pages read per outer batch.
///   2. For each outer batch, re-scan the full inner file and evaluate
///      predicates against every (inner, outer) pair.
///
/// Output column order is always [original_left | original_right] regardless of
/// which side was chosen as inner.
pub struct BlockNestedLoopJoinOperator<'a> {
    inner: Option<Box<dyn Operator + 'a>>,
    outer: Option<Box<dyn Operator + 'a>>,
    inner_is_left: bool,
    resolved: Vec<ResolvedPredicate>,
    merged_schema: RowSchema,
    /// Number of columns from the *left* relation in the merged schema.  Used
    /// by `eval_resolved_two_parts` to address each row half without merging.
    left_schema_len: usize,
    state: BlockNestedLoopState,
}

enum BlockNestedLoopState {
    NotStarted,
    NeedOuterBatch { inner_file: TempFileId },
    ScanningInner {
        inner_file: TempFileId,
        outer_batch: Vec<Row>,
        outer_batch_reserved_bytes: usize,
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
            left_schema_len: left_schema.len(),
            state: BlockNestedLoopState::NotStarted,
        })
    }

    fn spill_inner(&mut self, ctx: &mut ExecContext) -> Result<TempFileId> {
        let block_size = ctx.temp_storage.block_size().max(1);
        let batch_pages = (ctx.sort_run_bytes / block_size).max(1).min(256);
        let mut writer = TempRunWriter::with_batch_pages(ctx.temp_storage, batch_pages)?;
        let mut inner = self.inner.take().expect("inner already consumed");
        while let Some(row) = inner.next(ctx)? {
            writer.append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?;
        }
        writer.finish(ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer)
    }

    fn fill_outer_batch(&mut self, ctx: &mut ExecContext) -> Result<(Vec<Row>, usize)> {
        let target_bytes = choose_bnlj_outer_batch_bytes(ctx);
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        let outer = self.outer.as_mut().expect("outer already consumed");

        loop {
            let row = match outer.next(ctx)? {
                Some(r) => r,
                None => break,
            };

            let row_bytes = row.estimate_heap_size();

            // Check BEFORE push so we do not overshoot the batch target,
            // but always admit at least one row so the operator makes progress.
            if !batch.is_empty() && (bytes + row_bytes > target_bytes || ctx.available_memory() < row_bytes) {
                break;
            }

            ctx.try_reserve_memory(row_bytes)?;
            bytes += row_bytes;
            batch.push(row);
        }

        Ok((batch, bytes))
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
                    let (outer_batch, outer_batch_reserved_bytes) = self.fill_outer_batch(ctx)?;
                    if outer_batch.is_empty() {
                        ctx.temp_storage.delete_temp_file(inner_file)?;
                        self.state = BlockNestedLoopState::Done;
                        return Ok(None);
                    }

                    let inner_reader_pages = choose_reader_batch(ctx, 1);
                    let inner_reader = TempRunReader::with_batch_pages(
                        ctx.temp_storage,
                        inner_file,
                        inner_reader_pages,
                    )?;
                    self.state = BlockNestedLoopState::ScanningInner {
                        inner_file,
                        outer_batch,
                        outer_batch_reserved_bytes,
                        inner_reader,
                        output_buf: Vec::new(),
                    };
                }

                BlockNestedLoopState::ScanningInner {
                    inner_file,
                    outer_batch,
                    outer_batch_reserved_bytes,
                    mut inner_reader,
                    mut output_buf,
                } => {
                    if let Some(row) = output_buf.pop() {
                        self.state = BlockNestedLoopState::ScanningInner {
                            inner_file,
                            outer_batch,
                            outer_batch_reserved_bytes,
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
                            ctx.release_memory(outer_batch_reserved_bytes);
                            self.state = BlockNestedLoopState::NeedOuterBatch { inner_file };
                        }
                        Some(inner_row) => {
                            // Evaluate predicates BEFORE merging to avoid the
                            // Vec allocation + full-row clone for every pair
                            // that will be filtered out (the common case).
                            let left_len = self.left_schema_len;
                            if self.inner_is_left {
                                for outer_row in outer_batch.iter().rev() {
                                    if eval_resolved_two_parts(
                                        &inner_row, outer_row, left_len, &self.resolved,
                                    )? {
                                        output_buf.push(Row::merge(&inner_row, outer_row));
                                    }
                                }
                            } else {
                                for outer_row in outer_batch.iter().rev() {
                                    if eval_resolved_two_parts(
                                        outer_row, &inner_row, left_len, &self.resolved,
                                    )? {
                                        output_buf.push(Row::merge(outer_row, &inner_row));
                                    }
                                }
                            }
                            self.state = BlockNestedLoopState::ScanningInner {
                                inner_file,
                                outer_batch,
                                outer_batch_reserved_bytes,
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