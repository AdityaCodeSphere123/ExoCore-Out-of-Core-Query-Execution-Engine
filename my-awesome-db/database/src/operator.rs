use anyhow::Result;
use std::io::{BufRead, Write};
use crate::buffer_manager::BufferManager;
use crate::row::{Row, RowSchema};

use db_config::DbContext;

pub struct ExecContext<'a> {
    pub db_ctx: &'a DbContext,
    pub disk_reader: &'a mut dyn BufRead,
    pub disk_writer: &'a mut dyn Write,
    pub buffer_manager: &'a mut BufferManager,
}

pub trait Operator {
    fn schema(&self) -> &RowSchema;
    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Row>>;
}
