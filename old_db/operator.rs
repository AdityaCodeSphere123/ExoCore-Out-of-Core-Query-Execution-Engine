use anyhow::{bail, Result};
use std::io::{BufRead, Write};

use db_config::DbContext;

use crate::buffer_manager::BufferManager;
use crate::row::{Row, RowSchema};
use crate::temp_storage::TempStorageManager;

#[derive(Debug, Clone)]
pub struct QueryMemoryManager {
    budget_bytes: usize,
    used_bytes: usize,
    peak_used_bytes: usize,
}

impl QueryMemoryManager {
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            used_bytes: 0,
            peak_used_bytes: 0,
        }
    }

    pub fn try_reserve(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let Some(next_used) = self.used_bytes.checked_add(bytes) else {
            bail!("query memory accounting overflow while reserving {bytes} bytes");
        };
        if next_used > self.budget_bytes {
            bail!(
                "query memory budget exceeded: requested {} bytes, used {} bytes, budget {} bytes",
                bytes,
                self.used_bytes,
                self.budget_bytes
            );
        }
        self.used_bytes = next_used;
        self.peak_used_bytes = self.peak_used_bytes.max(self.used_bytes);
        Ok(())
    }

    pub fn release(&mut self, bytes: usize) {
        self.used_bytes = self.used_bytes.saturating_sub(bytes);
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    pub fn available_bytes(&self) -> usize {
        self.budget_bytes.saturating_sub(self.used_bytes)
    }

    pub fn peak_used_bytes(&self) -> usize {
        self.peak_used_bytes
    }
}

pub struct ExecContext<'a> {
    pub db_ctx: &'a DbContext,
    pub disk_reader: &'a mut dyn BufRead,
    pub disk_writer: &'a mut dyn Write,
    pub buffer_manager: &'a mut BufferManager,
    pub temp_storage: &'a mut TempStorageManager,
    pub sort_run_bytes: usize,
    pub memory: QueryMemoryManager,
}

impl<'a> ExecContext<'a> {
    pub fn try_reserve_memory(&mut self, bytes: usize) -> Result<()> {
        self.memory.try_reserve(bytes)
    }

    pub fn release_memory(&mut self, bytes: usize) {
        self.memory.release(bytes);
    }

    pub fn available_memory(&self) -> usize {
        self.memory.available_bytes()
    }

    pub fn used_memory(&self) -> usize {
        self.memory.used_bytes()
    }

    pub fn memory_budget(&self) -> usize {
        self.memory.budget_bytes()
    }
}

pub trait Operator {
    fn schema(&self) -> &RowSchema;
    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>>;
}