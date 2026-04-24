use anyhow::{anyhow, Result};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use common::Data;
use std::cmp::Ordering;

use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};

// ── resolved predicates (pre-computed column indices) ───────────────────────

/// A predicate with column names resolved to integer indices so that per-row
/// evaluation is O(1) instead of a linear scan through column names.
pub struct ResolvedPredicate {
    left_idx: usize,
    operator: ComparisionOperator,
    right: ResolvedRhs,
}

enum ResolvedRhs {
    Index(usize),
    Literal(Data),
}

/// Resolve a slice of predicates against a schema, converting column names
/// to integer indices.  Returns an error if any column name is missing.
pub fn resolve_predicates(
    schema: &RowSchema,
    predicates: &[Predicate],
) -> Result<Vec<ResolvedPredicate>> {
    predicates
        .iter()
        .map(|p| {
            let left_idx = schema.require_index(&p.column_name)?;
            let right = match &p.value {
                ComparisionValue::Column(c) => ResolvedRhs::Index(schema.require_index(c)?),
                ComparisionValue::I32(v) => ResolvedRhs::Literal(Data::Int32(*v)),
                ComparisionValue::I64(v) => ResolvedRhs::Literal(Data::Int64(*v)),
                ComparisionValue::F32(v) => ResolvedRhs::Literal(Data::Float32(*v)),
                ComparisionValue::F64(v) => ResolvedRhs::Literal(Data::Float64(*v)),
                ComparisionValue::String(v) => ResolvedRhs::Literal(Data::String(v.clone())),
            };
            Ok(ResolvedPredicate {
                left_idx,
                operator: p.operator.clone(),
                right,
            })
        })
        .collect()
}

/// Evaluate pre-resolved predicates against a row using direct index access.
///
/// Column indices were validated against the schema at construction time
/// (`resolve_predicates`), so out-of-bounds access is a programming error and
/// we skip the `get().ok_or_else()` indirection in the hot path.
pub fn eval_resolved(row: &Row, preds: &[ResolvedPredicate]) -> Result<bool> {
    let values = row.values();
    for pred in preds {
        // Safety: index validated by resolve_predicates at operator construction.
        debug_assert!(pred.left_idx < values.len(), "left_idx {} oob (len {})", pred.left_idx, values.len());
        let lv = &values[pred.left_idx];
        let rv = match &pred.right {
            ResolvedRhs::Index(idx) => {
                debug_assert!(*idx < values.len(), "right_idx {} oob (len {})", idx, values.len());
                &values[*idx]
            }
            ResolvedRhs::Literal(d) => d,
        };
        let ok = match &pred.operator {
            ComparisionOperator::EQ => lv == rv,
            ComparisionOperator::NE => lv != rv,
            _ => {
                let ord = compare_data_fast(lv, rv)?;
                match &pred.operator {
                    ComparisionOperator::LT => ord == Ordering::Less,
                    ComparisionOperator::LTE => ord != Ordering::Greater,
                    ComparisionOperator::GT => ord == Ordering::Greater,
                    ComparisionOperator::GTE => ord != Ordering::Less,
                    _ => unreachable!(),
                }
            }
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Evaluate predicates against two row halves *without* allocating a merged
/// `Row`.  Used by `BlockNestedLoopJoinOperator` to avoid the `Row::merge`
/// clone cost for every (inner, outer) pair that will be filtered out.
///
/// `left_len` is the number of columns contributed by `left`; predicate column
/// indices < `left_len` index into `left`, indices >= `left_len` index into
/// `right` (offset by `left_len`).
pub fn eval_resolved_two_parts(
    left: &Row,
    right: &Row,
    left_len: usize,
    preds: &[ResolvedPredicate],
) -> Result<bool> {
    let left_vals = left.values();
    let right_vals = right.values();

    #[inline(always)]
    fn pick<'a>(vals_l: &'a [Data], vals_r: &'a [Data], left_len: usize, idx: usize) -> &'a Data {
        if idx < left_len {
            debug_assert!(idx < vals_l.len());
            &vals_l[idx]
        } else {
            let ri = idx - left_len;
            debug_assert!(ri < vals_r.len());
            &vals_r[ri]
        }
    }

    for pred in preds {
        let lv = pick(left_vals, right_vals, left_len, pred.left_idx);
        let rv = match &pred.right {
            ResolvedRhs::Index(idx) => pick(left_vals, right_vals, left_len, *idx),
            ResolvedRhs::Literal(d) => d,
        };
        let ok = match &pred.operator {
            ComparisionOperator::EQ => lv == rv,
            ComparisionOperator::NE => lv != rv,
            _ => {
                let ord = compare_data_fast(lv, rv)?;
                match &pred.operator {
                    ComparisionOperator::LT => ord == Ordering::Less,
                    ComparisionOperator::LTE => ord != Ordering::Greater,
                    ComparisionOperator::GT => ord == Ordering::Greater,
                    ComparisionOperator::GTE => ord != Ordering::Less,
                    _ => unreachable!(),
                }
            }
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_data_fast(left: &Data, right: &Data) -> Result<Ordering> {
    match (left, right) {
        (Data::Int32(a), Data::Int32(b)) => Ok(a.cmp(b)),
        (Data::Int64(a), Data::Int64(b)) => Ok(a.cmp(b)),
        (Data::Float32(a), Data::Float32(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| anyhow!("cannot compare incompatible data types")),
        (Data::Float64(a), Data::Float64(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| anyhow!("cannot compare incompatible data types")),
        (Data::String(a), Data::String(b)) => Ok(a.cmp(b)),
        _ => left
            .partial_cmp(right)
            .ok_or_else(|| anyhow!("cannot compare incompatible data types")),
    }
}

// ── filter operator ─────────────────────────────────────────────────────────

pub struct FilterOperator<'a> {
    underlying: Box<dyn Operator + 'a>,
    resolved: Vec<ResolvedPredicate>,
}

impl<'a> FilterOperator<'a> {
    pub fn new(underlying: Box<dyn Operator + 'a>, predicates: &[Predicate]) -> Result<Self> {
        let resolved = resolve_predicates(underlying.schema(), predicates)?;
        Ok(Self {
            underlying,
            resolved,
        })
    }
}

impl<'a> Operator for FilterOperator<'a> {
    fn schema(&self) -> &RowSchema {
        self.underlying.schema()
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        while let Some(row) = self.underlying.next(ctx)? {
            if eval_resolved(&row, &self.resolved)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}