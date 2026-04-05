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
pub fn eval_resolved(row: &Row, preds: &[ResolvedPredicate]) -> Result<bool> {
    for pred in preds {
        let lv = row.require(pred.left_idx)?;
        let rv = match &pred.right {
            ResolvedRhs::Index(idx) => row.require(*idx)?,
            ResolvedRhs::Literal(d) => d,
        };
        let ok = match &pred.operator {
            ComparisionOperator::EQ => lv == rv,
            ComparisionOperator::NE => lv != rv,
            _ => {
                let ord = lv
                    .partial_cmp(rv)
                    .ok_or_else(|| anyhow!("cannot compare incompatible data types"))?;
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
