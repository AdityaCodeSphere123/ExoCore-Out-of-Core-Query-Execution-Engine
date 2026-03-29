use anyhow::{anyhow, Result};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use common::Data;

use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};

pub struct FilterOperator<'a> {
    underlying: Box<dyn Operator + 'a>,
    predicates: &'a [Predicate],
}

impl<'a> FilterOperator<'a> {
    pub fn new(underlying: Box<dyn Operator + 'a>, predicates: &'a [Predicate]) -> Self {
        Self {
            underlying,
            predicates,
        }
    }
}

impl<'a> Operator for FilterOperator<'a> {
    fn schema(&self) -> &RowSchema {
        self.underlying.schema()
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        while let Some(row) = self.underlying.next(ctx)? {
            let mut keep = true;
            for pred in self.predicates {
                if !evaluate_predicate(self.schema(), &row, pred)? {
                    keep = false;
                    break;
                }
            }
            if keep {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

fn evaluate_predicate(schema: &RowSchema, row: &Row, pred: &Predicate) -> Result<bool> {
    let left = row.get_by_name(schema, &pred.column_name)?;
    let right = resolve_rhs(schema, row, &pred.value)?;
    compare_data(left, &pred.operator, &right)
}

fn resolve_rhs(schema: &RowSchema, row: &Row, value: &ComparisionValue) -> Result<Data> {
    match value {
        ComparisionValue::Column(col_name) => Ok(row.get_by_name(schema, col_name)?.clone()),
        ComparisionValue::I32(v) => Ok(Data::Int32(*v)),
        ComparisionValue::I64(v) => Ok(Data::Int64(*v)),
        ComparisionValue::F32(v) => Ok(Data::Float32(*v)),
        ComparisionValue::F64(v) => Ok(Data::Float64(*v)),
        ComparisionValue::String(v) => Ok(Data::String(v.clone())),
    }
}

fn compare_data(left: &Data, op: &ComparisionOperator, right: &Data) -> Result<bool> {
    match op {
        ComparisionOperator::EQ => Ok(left == right),
        ComparisionOperator::NE => Ok(left != right),
        ComparisionOperator::LT => compare_ord(left, right, |o| o == std::cmp::Ordering::Less),
        ComparisionOperator::LTE => compare_ord(left, right, |o| {
            o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal
        }),
        ComparisionOperator::GT => compare_ord(left, right, |o| o == std::cmp::Ordering::Greater),
        ComparisionOperator::GTE => compare_ord(left, right, |o| {
            o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal
        }),
    }
}

fn compare_ord<F>(left: &Data, right: &Data, f: F) -> Result<bool>
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    let ord = left
        .partial_cmp(right)
        .ok_or_else(|| anyhow!("cannot compare incompatible data types"))?;
    Ok(f(ord))
}
