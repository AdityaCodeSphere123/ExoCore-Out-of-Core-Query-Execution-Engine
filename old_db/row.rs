use anyhow::{anyhow, bail, Result};
use common::Data;
use std::collections::HashMap;

// RowSchema stores a HashMap alongside the Vec so that index_of / contains are
// O(1) instead of O(n).  This matters because these are called on every row
// during filter evaluation, join key resolution, and column-pruning.
#[derive(Debug, Clone)]
pub struct RowSchema {
    column_names: Vec<String>,
    name_to_idx: HashMap<String, usize>,
}

impl RowSchema {
    pub fn new(column_names: Vec<String>) -> Self {
        let name_to_idx = column_names
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), i))
            .collect();
        Self { column_names, name_to_idx }
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
        self.name_to_idx.get(column_name).copied()
    }

    pub fn require_index(&self, column_name: &str) -> Result<usize> {
        self.index_of(column_name)
            .ok_or_else(|| anyhow!("column not found in schema: {}", column_name))
    }

    pub fn contains(&self, column_name: &str) -> bool {
        self.name_to_idx.contains_key(column_name)
    }

    pub fn push_column(&mut self, column_name: impl Into<String>) {
        let col = column_name.into();
        let idx = self.column_names.len();
        self.name_to_idx.insert(col.clone(), idx);
        self.column_names.push(col);
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

    pub fn project_by_indices_owned(self, indices: &[usize]) -> Result<Row> {
        let Row { values } = self;

        // Fast path 1: identity projection — return values unchanged.
        if indices.len() == values.len() && indices.iter().enumerate().all(|(i, &idx)| i == idx) {
            return Ok(Row::new(values));
        }

        // Fast path 2: sorted unique indices.
        if indices.windows(2).all(|w| w[0] < w[1]) {
            let mut out = Vec::with_capacity(indices.len());
            let mut src = values.into_iter().enumerate();
            for &target in indices {
                loop {
                    match src.next() {
                        Some((i, v)) if i == target => {
                            out.push(v);
                            break;
                        }
                        Some(_) => {}
                        None => return Err(anyhow!("row index out of bounds: {}", target)),
                    }
                }
            }
            return Ok(Row::new(out));
        }

        // Fallback: allow duplicates / arbitrary order by cloning.
        let mut out = Vec::with_capacity(indices.len());
        for &idx in indices {
            out.push(
                values
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| anyhow!("row index out of bounds: {}", idx))?
            );
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

    /// Estimate the total heap memory this row occupies, including the Vec
    /// backing store and any String heap allocations.  Used by sort and join
    /// operators to budget memory usage.  Previously duplicated in sort.rs and
    /// join.rs; canonical home is here.
    pub fn estimate_heap_size(&self) -> usize {
        use std::mem::size_of;
        // Row struct + Vec<Data> elements + Vec allocator metadata
        let mut total = size_of::<Row>() + self.values.len() * size_of::<Data>() + 16;
        for value in &self.values {
            if let Data::String(s) = value {
                total += s.capacity() + 16; // +16 for String's heap allocator metadata
            }
        }
        total
    }

    /// Append this row's pipe-delimited representation to an existing String.
    /// Callers should prefer this over `to_pipe_string` when a reusable buffer
    /// is available — it eliminates the per-row heap allocation entirely.
    pub fn append_pipe_to(&self, out: &mut String) {
        use std::fmt::Write as FmtWrite;
        for val in &self.values {
            match val {
                Data::Int32(v) => { let _ = write!(out, "{}", v); }
                Data::Int64(v) => { let _ = write!(out, "{}", v); }
                Data::Float32(v) => {
                    let s = v.to_string();
                    out.push_str(&s);
                    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
                        out.push_str(".0");
                    }
                }
                Data::Float64(v) => {
                    let s = v.to_string();
                    out.push_str(&s);
                    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
                        out.push_str(".0");
                    }
                }
                Data::String(v) => out.push_str(v),
            }
            out.push('|');
        }
        out.push('\n');
    }

    pub fn to_pipe_string(&self) -> String {
        let mut out = String::new();
        self.append_pipe_to(&mut out);
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
        Data::Float32(v) => {
            let s = v.to_string();
            if s.contains('.') || s.contains('e') || s.contains('E') {
                s
            } else {
                format!("{s}.0")
            }
        }
        Data::Float64(v) => {
            let s = v.to_string();
            if s.contains('.') || s.contains('e') || s.contains('E') {
                s
            } else {
                format!("{s}.0")
            }
        }
        Data::String(v) => v.clone(),
    }
}