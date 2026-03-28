use anyhow::Result;
use crate::row::{Row, RowSchema};

pub fn apply_project(
    input_schema: &RowSchema,
    input_rows: Vec<Row>,
    column_name_map: &Vec<(String, String)>,
) -> Result<(RowSchema, Vec<Row>)> {
    let mut indices = Vec::with_capacity(column_name_map.len());
    let mut output_columns = Vec::with_capacity(column_name_map.len());

    for (source_col, alias) in column_name_map {
        let idx = input_schema.require_index(source_col)?;
        indices.push(idx);
        output_columns.push(alias.clone());
    }

    let output_schema = RowSchema::new(output_columns);

    let mut output_rows = Vec::with_capacity(input_rows.len());
    for row in input_rows {
        output_rows.push(row.project_by_indices(&indices)?);
    }

    Ok((output_schema, output_rows))
}