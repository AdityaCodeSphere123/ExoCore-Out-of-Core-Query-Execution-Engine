// Implements the projection operator for selecting specific columns from input rows.

use anyhow::Result;

use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};

pub struct ProjectOperator<'a> {
    underlying: Box<dyn Operator + 'a>,
    indices: Vec<usize>,
    schema: RowSchema,
}

impl<'a> ProjectOperator<'a> {
    pub fn new(
        underlying: Box<dyn Operator + 'a>,
        column_name_map: &[(String, String)],
    ) -> Result<Self> {
        let input_schema = underlying.schema();
        let mut indices = Vec::with_capacity(column_name_map.len());
        let mut output_columns = Vec::with_capacity(column_name_map.len());

        for (source_col, alias) in column_name_map {
            let idx = input_schema.require_index(source_col)?;
            indices.push(idx);
            output_columns.push(alias.clone());
        }

        Ok(Self {
            underlying,
            indices,
            schema: RowSchema::new(output_columns),
        })
    }
}

impl<'a> Operator for ProjectOperator<'a> {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        match self.underlying.next(ctx)? {
            Some(row) => Ok(Some(row.project_by_indices_owned(&self.indices)?)),
            None => Ok(None),
        }
    }
}
