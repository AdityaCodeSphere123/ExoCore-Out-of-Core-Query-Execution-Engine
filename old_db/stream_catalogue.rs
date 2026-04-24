/// Streaming catalog: accumulates file-level statistics as rows flow through
/// scan operators during query execution.  The stats are built on-the-fly as a
/// zero-extra-I/O byproduct of the ordinary block-decode pass, so they are free
/// to collect.
///
/// The catalog is keyed by table_id (same identifier used throughout the
/// codebase).  Per-column tracking covers:
///   * min / max value  (→ RangeStat equivalent)
///   * monotone-order detection  (→ IsPhysicallyOrdered equivalent)
///   * row count  (→ CardinalityStat equivalent)
///
/// These can be used by:
///   * Operators later in the same pipeline to estimate cardinality / ranges
///     when the static StatsCatalog has no entry for that table.
///   * Debugging / introspection of what the engine actually saw.

use std::collections::HashMap;
use common::Data;

// ── per-column live stats ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StreamingColumnStats {
    pub name: String,
    pub min_val: Option<Data>,
    pub max_val: Option<Data>,
    /// False the moment a value smaller than its predecessor is observed.
    pub is_ordered: bool,
    /// Last value seen, used to check ordering on the next row.
    prev_val: Option<Data>,
}

impl StreamingColumnStats {
    fn new(name: String) -> Self {
        Self {
            name,
            min_val: None,
            max_val: None,
            is_ordered: true,
            prev_val: None,
        }
    }

    /// Update stats with one observed column value.  O(1).
    pub fn observe(&mut self, value: &Data) {
        // min
        match &self.min_val {
            None => self.min_val = Some(value.clone()),
            Some(m) => {
                if let Some(std::cmp::Ordering::Less) = value.partial_cmp(m) {
                    self.min_val = Some(value.clone());
                }
            }
        }
        // max
        match &self.max_val {
            None => self.max_val = Some(value.clone()),
            Some(m) => {
                if let Some(std::cmp::Ordering::Greater) = value.partial_cmp(m) {
                    self.max_val = Some(value.clone());
                }
            }
        }
        // ordering: once broken, stays broken
        if self.is_ordered {
            if let Some(ref prev) = self.prev_val {
                if let Some(ord) = value.partial_cmp(prev) {
                    if ord == std::cmp::Ordering::Less {
                        self.is_ordered = false;
                    }
                }
            }
        }
        self.prev_val = Some(value.clone());
    }
}

// ── per-table live stats ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StreamingTableStats {
    pub row_count: u64,
    /// One entry per column in the original (un-pruned) table schema, in schema
    /// order.  Columns that were pruned away (not kept by the scan) will have
    /// min/max = None but is_ordered will still be `true` (it was never
    /// invalidated).
    pub columns: Vec<StreamingColumnStats>,
}

impl StreamingTableStats {
    fn new(col_names: &[String]) -> Self {
        Self {
            row_count: 0,
            columns: col_names.iter().map(|n| StreamingColumnStats::new(n.clone())).collect(),
        }
    }
}

// ── catalog ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct StreamingCatalog {
    tables: HashMap<String, StreamingTableStats>,
}

impl StreamingCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table before scanning begins.  Idempotent: if the table is
    /// already registered the call is a no-op so repeated scans are safe.
    pub fn init_table(&mut self, table_id: &str, col_names: &[String]) {
        self.tables
            .entry(table_id.to_string())
            .or_insert_with(|| StreamingTableStats::new(col_names));
    }

    /// Record one value for a specific (table, original-column-index) pair.
    /// Called during block decode for every value that is read (kept columns).
    /// `orig_col_idx` is the column's index in the *full* table schema.
    #[inline]
    pub fn observe(&mut self, table_id: &str, orig_col_idx: usize, value: &Data) {
        if let Some(table) = self.tables.get_mut(table_id) {
            if let Some(col) = table.columns.get_mut(orig_col_idx) {
                col.observe(value);
            }
        }
    }

    /// Increment the row counter for a table.  Called once per decoded row.
    #[inline]
    pub fn count_row(&mut self, table_id: &str) {
        if let Some(table) = self.tables.get_mut(table_id) {
            table.row_count += 1;
        }
    }

    pub fn get_table(&self, table_id: &str) -> Option<&StreamingTableStats> {
        self.tables.get(table_id)
    }

    pub fn get_column(&self, table_id: &str, col_name: &str) -> Option<&StreamingColumnStats> {
        let table = self.tables.get(table_id)?;
        table.columns.iter().find(|c| c.name == col_name)
    }

    /// Estimated row count for a table from streaming stats, if available.
    pub fn row_count(&self, table_id: &str) -> Option<f64> {
        let t = self.tables.get(table_id)?;
        if t.row_count > 0 { Some(t.row_count as f64) } else { None }
    }

    /// Whether streaming stats suggest the column is physically ordered.
    pub fn is_ordered(&self, table_id: &str, col_name: &str) -> Option<bool> {
        let col = self.get_column(table_id, col_name)?;
        // Only meaningful once we've seen at least 2 rows.
        let table = self.tables.get(table_id)?;
        if table.row_count < 2 { return None; }
        Some(col.is_ordered)
    }
}