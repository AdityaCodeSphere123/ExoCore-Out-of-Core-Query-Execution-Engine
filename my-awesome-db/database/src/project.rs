use anyhow::Result;
use crate::operator::{ExecContext, Operator};
use crate::row::{Row, RowSchema};

pub struct ProjectOperator {
    underlying: Box<dyn Operator>,
    indices: Vec<usize>,
    schema: RowSchema,
}

impl ProjectOperator {
    pub fn new(underlying: Box<dyn Operator>, column_name_map: &Vec<(String, String)>) -> Result<Self> {
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

impl Operator for ProjectOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>> {
        if let Some(row) = self.underlying.next(ctx)? {
            Ok(Some(row.project_by_indices(&self.indices)?))
        } else {
            Ok(None)
        }
    }
}