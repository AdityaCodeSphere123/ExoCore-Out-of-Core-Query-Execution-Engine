use anyhow::{anyhow, Result};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use common::Data;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

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

// ── predicate evaluation on merged rows ──────────────────────────────────────

fn eval_predicates(row: &Row, schema: &RowSchema, predicates: &[Predicate]) -> Result<bool> {
    for pred in predicates {
        let lv = row.get_by_name(schema, &pred.column_name)?;
        let rv = match &pred.value {
            ComparisionValue::Column(c) => row.get_by_name(schema, c)?.clone(),
            ComparisionValue::I32(v) => Data::Int32(*v),
            ComparisionValue::I64(v) => Data::Int64(*v),
            ComparisionValue::F32(v) => Data::Float32(*v),
            ComparisionValue::F64(v) => Data::Float64(*v),
            ComparisionValue::String(v) => Data::String(v.clone()),
        };
        let ok = match &pred.operator {
            ComparisionOperator::EQ => lv == &rv,
            ComparisionOperator::NE => lv != &rv,
            ComparisionOperator::LT => {
                lv.partial_cmp(&rv)
                    .ok_or_else(|| anyhow!("incompatible types in join predicate"))?
                    == std::cmp::Ordering::Less
            }
            ComparisionOperator::LTE => matches!(
                lv.partial_cmp(&rv)
                    .ok_or_else(|| anyhow!("incompatible types in join predicate"))?,
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            ),
            ComparisionOperator::GT => {
                lv.partial_cmp(&rv)
                    .ok_or_else(|| anyhow!("incompatible types in join predicate"))?
                    == std::cmp::Ordering::Greater
            }
            ComparisionOperator::GTE => matches!(
                lv.partial_cmp(&rv)
                    .ok_or_else(|| anyhow!("incompatible types in join predicate"))?,
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            ),
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
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
        )))
    } else {
        Ok(Box::new(CrossJoinOperator::new(
            left,
            right,
            extra_predicates,
        )))
    }
}

// ── Grace Hash Join ───────────────────────────────────────────────────────────

pub struct HashJoinOperator<'a> {
    left: Option<Box<dyn Operator + 'a>>,
    right: Option<Box<dyn Operator + 'a>>,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    extra_predicates: Vec<Predicate>,
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
    ) -> Self {
        let left_schema = left.schema().clone();
        let right_schema = right.schema().clone();
        let merged_schema = RowSchema::merge(&left_schema, &right_schema);
        Self {
            left: Some(left),
            right: Some(right),
            left_key_indices,
            right_key_indices,
            extra_predicates,
            left_schema,
            merged_schema,
            state: HashJoinState::NotStarted,
        }
    }

    /// Partition both inputs into `num_partitions` temp files.
    /// All rows with the same join key hash go to the same partition.
    fn partition_inputs(
        &mut self,
        ctx: &mut ExecContext,
    ) -> Result<(Vec<TempFileId>, Vec<TempFileId>)> {
        let block_size = ctx.temp_storage.block_size();
        // Each writer owns one block-sized page buffer. We allocate 2×N writers
        // (left + right), so cap at sort_run_bytes / (2 × block_size).
        let num_partitions = (ctx.sort_run_bytes / (2 * block_size)).clamp(4, 256);

        let mut left_writers: Vec<TempRunWriter> = (0..num_partitions)
            .map(|_| TempRunWriter::new(ctx.temp_storage))
            .collect::<Result<_>>()?;
        let mut right_writers: Vec<TempRunWriter> = (0..num_partitions)
            .map(|_| TempRunWriter::new(ctx.temp_storage))
            .collect::<Result<_>>()?;

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
            left_writers[part].append_row(
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
            right_writers[part].append_row(
                &row,
                ctx.temp_storage,
                &mut *ctx.disk_reader,
                &mut *ctx.disk_writer,
            )?;
        }
        drop(right);

        // Flush and seal all partition files.
        let left_parts: Vec<TempFileId> = left_writers
            .into_iter()
            .map(|w| w.finish(ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer))
            .collect::<Result<_>>()?;
        let right_parts: Vec<TempFileId> = right_writers
            .into_iter()
            .map(|w| w.finish(ctx.temp_storage, &mut *ctx.disk_reader, &mut *ctx.disk_writer))
            .collect::<Result<_>>()?;

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
                        ps.probe_reader =
                            Some(TempRunReader::new(ctx.temp_storage, right_file)?);
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
                                    if eval_predicates(
                                        &merged,
                                        &self.merged_schema,
                                        &self.extra_predicates,
                                    )? {
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
    predicates: Vec<Predicate>,
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
    ) -> Self {
        let left_schema = left.schema().clone();
        let right_schema = right.schema().clone();
        let merged_schema = RowSchema::merge(&left_schema, &right_schema);
        Self {
            left: Some(left),
            right: Some(right),
            predicates,
            merged_schema,
            state: CrossState::NotStarted,
        }
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
                            let left_reader =
                                TempRunReader::new(ctx.temp_storage, left_file)?;
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
                            if eval_predicates(&merged, &self.merged_schema, &self.predicates)? {
                                return Ok(Some(merged));
                            }
                        }
                    }
                }
            }
        }
    }
}
