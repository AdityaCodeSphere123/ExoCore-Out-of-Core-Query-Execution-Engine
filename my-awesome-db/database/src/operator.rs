use anyhow::Result;
use std::io::{BufRead, Write};

use db_config::DbContext;

use crate::buffer_manager::BufferManager;
use crate::row::{Row, RowSchema};
use crate::temp_storage::TempStorageManager;

pub struct ExecContext<'a> {
    pub db_ctx: &'a DbContext,
    pub disk_reader: &'a mut dyn BufRead,
    pub disk_writer: &'a mut dyn Write,
    pub buffer_manager: &'a mut BufferManager,
    pub temp_storage: &'a mut TempStorageManager,
    pub sort_run_bytes: usize,
}

pub trait Operator {
    fn schema(&self) -> &RowSchema;
    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>>;
}
