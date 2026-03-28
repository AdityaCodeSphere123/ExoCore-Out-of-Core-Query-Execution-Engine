use anyhow::{anyhow, bail, Result};
use common::Data;

#[derive(Debug, Clone)]
pub struct RowSchema {
    column_names: Vec<String>,
}

impl RowSchema {
    pub fn new(column_names: Vec<String>) -> Self {
        Self { column_names }
    }

    pub fn len(&self) -> usize {
        self.column_names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.column_names.is_empty()
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    pub fn index_of(&self, column_name: &str) -> Option<usize> {
        self.column_names.iter().position(|c| c == column_name)
    }

    pub fn require_index(&self, column_name: &str) -> Result<usize> {
        self.index_of(column_name)
            .ok_or_else(|| anyhow!("column not found in schema: {}", column_name))
    }

    pub fn contains(&self, column_name: &str) -> bool {
        self.index_of(column_name).is_some()
    }

    pub fn push_column(&mut self, column_name: impl Into<String>) {
        self.column_names.push(column_name.into());
    }

    pub fn project(&self, selected_columns: &[String]) -> Result<Self> {
        for col in selected_columns {
            self.require_index(col)?;
        }
        Ok(Self::new(selected_columns.to_vec()))
    }

    pub fn merge(left: &RowSchema, right: &RowSchema) -> Self {
        let mut cols = Vec::with_capacity(left.len() + right.len());
        cols.extend(left.column_names.iter().cloned());
        cols.extend(right.column_names.iter().cloned());
        Self::new(cols)
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<Data>,
}

impl Row {
    pub fn new(values: Vec<Data>) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[Data] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Data> {
        self.values
    }

    pub fn get(&self, idx: usize) -> Option<&Data> {
        self.values.get(idx)
    }

    pub fn require(&self, idx: usize) -> Result<&Data> {
        self.values
            .get(idx)
            .ok_or_else(|| anyhow!("row index out of bounds: {}", idx))
    }

    pub fn get_by_name<'a>(&'a self, schema: &RowSchema, column_name: &str) -> Result<&'a Data> {
        let idx = schema.require_index(column_name)?;
        self.require(idx)
    }

    pub fn project_by_indices(&self, indices: &[usize]) -> Result<Row> {
        let mut out = Vec::with_capacity(indices.len());
        for &idx in indices {
            out.push(self.require(idx)?.clone());
        }
        Ok(Row::new(out))
    }

    pub fn project_by_names(&self, schema: &RowSchema, column_names: &[String]) -> Result<Row> {
        let mut indices = Vec::with_capacity(column_names.len());
        for col in column_names {
            indices.push(schema.require_index(col)?);
        }
        self.project_by_indices(&indices)
    }

    pub fn merge(left: &Row, right: &Row) -> Row {
        let mut vals = Vec::with_capacity(left.len() + right.len());
        vals.extend(left.values.iter().cloned());
        vals.extend(right.values.iter().cloned());
        Row::new(vals)
    }

    pub fn to_pipe_string(&self) -> String {
        let mut out = String::new();
        for val in &self.values {
            out.push_str(&data_to_string(val));
            out.push('|');
        }
        out.push('\n');
        out
    }

    pub fn validate_against_schema(&self, schema: &RowSchema) -> Result<()> {
        if self.len() != schema.len() {
            bail!(
                "row/schema length mismatch: row has {} values, schema has {} columns",
                self.len(),
                schema.len()
            );
        }
        Ok(())
    }
}

pub fn data_to_string(data: &Data) -> String {
    match data {
        Data::Int32(v) => v.to_string(),
        Data::Int64(v) => v.to_string(),
        Data::Float32(v) => v.to_string(),
        Data::Float64(v) => v.to_string(),
        Data::String(v) => v.clone(),
    }
}