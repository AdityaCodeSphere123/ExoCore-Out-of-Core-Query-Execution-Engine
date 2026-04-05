use anyhow::{anyhow, bail, Result};
use common::Data;
use std::collections::HashMap;
use std::io::{BufRead, Write};

use crate::row::Row;

const DEFAULT_TEMP_IO_BATCH_PAGES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TempFileId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TempPageId {
    pub file_id: TempFileId,
    pub page_index: u64,
}

#[derive(Debug, Clone, Copy)]
struct TempExtent {
    start_block: u64,
    num_pages: u64,
}

#[derive(Debug, Default)]
struct TempFileMeta {
    extents: Vec<TempExtent>,
    num_pages: u64,
}

pub struct TempStorageManager {
    block_size: usize,
    next_file_id: u64,
    next_free_block: Option<u64>,
    files: HashMap<TempFileId, TempFileMeta>,
}

impl TempStorageManager {
    pub fn new(block_size: usize) -> Result<Self> {
        if block_size < 64 {
            bail!("block size too small for temp storage: {}", block_size);
        }

        Ok(Self {
            block_size,
            next_file_id: 0,
            next_free_block: None,
            files: HashMap::new(),
        })
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn create_temp_file(&mut self) -> Result<TempFileId> {
        let file_id = TempFileId(self.next_file_id);
        self.next_file_id += 1;
        self.files.insert(file_id, TempFileMeta::default());
        Ok(file_id)
    }

    pub fn delete_temp_file(&mut self, file_id: TempFileId) -> Result<()> {
        self.files
            .remove(&file_id)
            .ok_or_else(|| anyhow!("unknown temp file id {}", file_id.0))?;
        Ok(())
    }

    pub fn num_pages(&self, file_id: TempFileId) -> Result<u64> {
        Ok(self.file_meta(file_id)?.num_pages)
    }

    pub fn allocate_page<RDisk, WDisk>(
        &mut self,
        file_id: TempFileId,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<TempPageId>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        let start = self.allocate_extent(file_id, 1, disk_reader, disk_writer)?;
        Ok(TempPageId {
            file_id,
            page_index: start.page_index,
        })
    }

    pub fn read_page<RDisk, WDisk>(
        &self,
        page_id: TempPageId,
        out: &mut [u8],
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        self.read_pages(page_id, 1, out, disk_reader, disk_writer)
    }

    pub fn read_pages<RDisk, WDisk>(
        &self,
        start_page: TempPageId,
        num_pages: u64,
        out: &mut [u8],
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if start_page.file_id.0 >= self.next_file_id {
            bail!("unknown temp file id {}", start_page.file_id.0);
        }
        if num_pages == 0 {
            if !out.is_empty() {
                bail!("non-empty output buffer provided for zero-page read");
            }
            return Ok(());
        }

        let expected_len = (num_pages as usize)
            .checked_mul(self.block_size)
            .ok_or_else(|| anyhow!("read_pages buffer size overflow for {} pages", num_pages))?;
        if out.len() != expected_len {
            bail!(
                "read_pages buffer length {} does not match {} pages of block size {}",
                out.len(),
                num_pages,
                self.block_size
            );
        }

        let (extent, page_offset_in_extent) =
            self.locate_extent(start_page.file_id, start_page.page_index)?;
        if page_offset_in_extent + num_pages > extent.num_pages {
            bail!(
                "read_pages requested {} pages from page {} of temp file {}, crossing extent boundary",
                num_pages,
                start_page.page_index,
                start_page.file_id.0
            );
        }

        let start_block = extent.start_block + page_offset_in_extent;
        get_blocks(disk_reader, disk_writer, start_block, num_pages, out)
    }

    pub fn write_page<RDisk, WDisk>(
        &self,
        page_id: TempPageId,
        data: &[u8],
        _disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        self.write_pages(page_id, 1, data, disk_writer)
    }

    pub fn write_pages<WDisk>(
        &self,
        start_page: TempPageId,
        num_pages: u64,
        data: &[u8],
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        WDisk: Write + ?Sized,
    {
        if num_pages == 0 {
            if !data.is_empty() {
                bail!("non-empty data buffer provided for zero-page write");
            }
            return Ok(());
        }

        let expected_len = (num_pages as usize)
            .checked_mul(self.block_size)
            .ok_or_else(|| anyhow!("write_pages buffer size overflow for {} pages", num_pages))?;
        if data.len() != expected_len {
            bail!(
                "write_pages data length {} does not match {} pages of block size {}",
                data.len(),
                num_pages,
                self.block_size
            );
        }

        let (extent, page_offset_in_extent) =
            self.locate_extent(start_page.file_id, start_page.page_index)?;
        if page_offset_in_extent + num_pages > extent.num_pages {
            bail!(
                "write_pages requested {} pages from page {} of temp file {}, crossing extent boundary",
                num_pages,
                start_page.page_index,
                start_page.file_id.0
            );
        }

        let start_block = extent.start_block + page_offset_in_extent;
        put_blocks(disk_writer, start_block, num_pages, data)
    }

    fn allocate_extent<RDisk, WDisk>(
        &mut self,
        file_id: TempFileId,
        num_pages: u64,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<TempPageId>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if num_pages == 0 {
            bail!("cannot allocate zero-page extent");
        }

        self.ensure_anon_region_initialized(disk_reader, disk_writer)?;

        let start_block = self
            .next_free_block
            .ok_or_else(|| anyhow!("anonymous region start block not initialized"))?;
        let next_block = start_block
            .checked_add(num_pages)
            .ok_or_else(|| anyhow!("anonymous region block id overflow"))?;
        self.next_free_block = Some(next_block);

        let meta = self.file_meta_mut(file_id)?;
        let start_page_index = meta.num_pages;
        meta.extents.push(TempExtent {
            start_block,
            num_pages,
        });
        meta.num_pages = meta
            .num_pages
            .checked_add(num_pages)
            .ok_or_else(|| anyhow!("temp file page count overflow for file {}", file_id.0))?;

        Ok(TempPageId {
            file_id,
            page_index: start_page_index,
        })
    }

    fn ensure_anon_region_initialized<RDisk, WDisk>(
        &mut self,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if self.next_free_block.is_none() {
            self.next_free_block = Some(get_anon_start_block(disk_reader, disk_writer)?);
        }
        Ok(())
    }

    fn locate_extent(&self, file_id: TempFileId, page_index: u64) -> Result<(TempExtent, u64)> {
        let meta = self.file_meta(file_id)?;
        if page_index >= meta.num_pages {
            bail!(
                "page {} out of bounds for temp file {} with {} pages",
                page_index,
                file_id.0,
                meta.num_pages
            );
        }

        let mut prefix_pages = 0u64;
        for extent in &meta.extents {
            if page_index < prefix_pages + extent.num_pages {
                return Ok((*extent, page_index - prefix_pages));
            }
            prefix_pages += extent.num_pages;
        }

        bail!(
            "page {} could not be mapped to an extent for temp file {}",
            page_index,
            file_id.0
        )
    }

    fn file_meta(&self, file_id: TempFileId) -> Result<&TempFileMeta> {
        self.files
            .get(&file_id)
            .ok_or_else(|| anyhow!("unknown temp file id {}", file_id.0))
    }

    fn file_meta_mut(&mut self, file_id: TempFileId) -> Result<&mut TempFileMeta> {
        self.files
            .get_mut(&file_id)
            .ok_or_else(|| anyhow!("unknown temp file id {}", file_id.0))
    }
}

/// A writer for a single sorted run in temp storage.
///
/// Unlike `TempRunReader`, this struct does NOT borrow `TempStorageManager`.
/// Instead, `storage` is passed as a parameter to each method.  This lets
/// multiple `TempRunWriter`s coexist (needed during Grace Hash Join
/// partitioning where we maintain one writer per partition simultaneously).
pub struct TempRunWriter {
    file_id: TempFileId,
    io_batch_pages: usize,
    page_buf: Vec<u8>,
    page_offset: usize,
    row_count: u16,
    pending_pages_buf: Vec<u8>,
    pending_pages: u64,
    finished: bool,
}

impl TempRunWriter {
    pub fn new(storage: &mut TempStorageManager) -> Result<Self> {
        Self::with_batch_pages(storage, DEFAULT_TEMP_IO_BATCH_PAGES)
    }

    pub fn with_batch_pages(
        storage: &mut TempStorageManager,
        io_batch_pages: usize,
    ) -> Result<Self> {
        if io_batch_pages == 0 {
            bail!("TempRunWriter batch size must be > 0");
        }

        let file_id = storage.create_temp_file()?;
        let block_size = storage.block_size();
        let mut pending_pages_buf = Vec::with_capacity(
            block_size
                .checked_mul(io_batch_pages)
                .ok_or_else(|| anyhow!("pending write buffer capacity overflow"))?,
        );
        pending_pages_buf.clear();

        Ok(Self {
            file_id,
            io_batch_pages,
            page_buf: vec![0u8; block_size],
            page_offset: 0,
            row_count: 0,
            pending_pages_buf,
            pending_pages: 0,
            finished: false,
        })
    }

    pub fn file_id(&self) -> TempFileId {
        self.file_id
    }

    pub fn append_row<RDisk, WDisk>(
        &mut self,
        row: &Row,
        storage: &mut TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if self.finished {
            bail!("cannot append to finalized temp run {}", self.file_id.0);
        }

        let encoded = encode_row(row)?;
        let record_len = 4 + encoded.len();
        let usable_end = self.page_buf.len() - 2;

        if record_len > usable_end {
            bail!(
                "single row of {} bytes is too large for temp page of usable size {}",
                record_len,
                usable_end
            );
        }

        if self.page_offset + record_len > usable_end {
            self.flush_current_page(storage, disk_reader, disk_writer)?;
        }

        let len_bytes = (encoded.len() as u32).to_le_bytes();
        self.page_buf[self.page_offset..self.page_offset + 4].copy_from_slice(&len_bytes);
        self.page_offset += 4;
        self.page_buf[self.page_offset..self.page_offset + encoded.len()]
            .copy_from_slice(&encoded);
        self.page_offset += encoded.len();
        self.row_count += 1;
        Ok(())
    }

    pub fn finish<RDisk, WDisk>(
        mut self,
        storage: &mut TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<TempFileId>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if !self.finished {
            if self.row_count > 0 || (self.pending_pages == 0 && storage.num_pages(self.file_id)? == 0) {
                self.flush_current_page(storage, disk_reader, disk_writer)?;
            }
            self.flush_pending_pages(storage, disk_reader, disk_writer)?;
            self.finished = true;
        }
        Ok(self.file_id)
    }

    fn flush_current_page<RDisk, WDisk>(
        &mut self,
        storage: &mut TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        let usable_end = self.page_buf.len() - 2;
        self.page_buf[usable_end..usable_end + 2].copy_from_slice(&self.row_count.to_le_bytes());

        self.pending_pages_buf.extend_from_slice(&self.page_buf);
        self.pending_pages += 1;

        if self.pending_pages >= self.io_batch_pages as u64 {
            self.flush_pending_pages(storage, disk_reader, disk_writer)?;
        }

        self.page_buf.fill(0);
        self.page_offset = 0;
        self.row_count = 0;
        Ok(())
    }

    fn flush_pending_pages<RDisk, WDisk>(
        &mut self,
        storage: &mut TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if self.pending_pages == 0 {
            return Ok(());
        }

        let start_page = storage.allocate_extent(
            self.file_id,
            self.pending_pages,
            disk_reader,
            disk_writer,
        )?;
        storage.write_pages(
            start_page,
            self.pending_pages,
            &self.pending_pages_buf,
            disk_writer,
        )?;

        self.pending_pages_buf.clear();
        self.pending_pages = 0;
        Ok(())
    }
}

pub struct TempRunReader {
    block_size: usize,
    file_id: TempFileId,
    num_pages: u64,
    io_batch_pages: usize,
    current_page_index: u64,
    page_buf: Vec<u8>,
    batch_buf: Vec<u8>,
    batch_pages: u64,
    next_page_in_batch: u64,
    page_offset: usize,
    rows_remaining_in_page: usize,
}

impl TempRunReader {
    pub fn new(storage: &TempStorageManager, file_id: TempFileId) -> Result<Self> {
        Self::with_batch_pages(storage, file_id, DEFAULT_TEMP_IO_BATCH_PAGES)
    }

    pub fn with_batch_pages(
        storage: &TempStorageManager,
        file_id: TempFileId,
        io_batch_pages: usize,
    ) -> Result<Self> {
        if io_batch_pages == 0 {
            bail!("TempRunReader batch size must be > 0");
        }

        let block_size = storage.block_size();
        let batch_cap = block_size
            .checked_mul(io_batch_pages)
            .ok_or_else(|| anyhow!("batch read buffer capacity overflow"))?;

        Ok(Self {
            block_size,
            file_id,
            num_pages: storage.num_pages(file_id)?,
            io_batch_pages,
            current_page_index: 0,
            page_buf: vec![0u8; block_size],
            batch_buf: Vec::with_capacity(batch_cap),
            batch_pages: 0,
            next_page_in_batch: 0,
            page_offset: 0,
            rows_remaining_in_page: 0,
        })
    }

    pub fn next_row<RDisk, WDisk>(
        &mut self,
        storage: &TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<Option<Row>>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        loop {
            if self.rows_remaining_in_page > 0 {
                let row = decode_row_from_page(&self.page_buf, &mut self.page_offset)?;
                self.rows_remaining_in_page -= 1;
                return Ok(Some(row));
            }

            if self.current_page_index >= self.num_pages {
                return Ok(None);
            }

            self.load_next_page(storage, disk_reader, disk_writer)?;
        }
    }

    fn load_next_page<RDisk, WDisk>(
        &mut self,
        storage: &TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if self.next_page_in_batch >= self.batch_pages {
            self.refill_batch(storage, disk_reader, disk_writer)?;
        }

        let page_idx_within_batch = self.next_page_in_batch as usize;
        let start = page_idx_within_batch * self.block_size;
        let end = start + self.block_size;
        self.page_buf.copy_from_slice(&self.batch_buf[start..end]);

        self.current_page_index += 1;
        self.next_page_in_batch += 1;

        let row_count_idx = self.block_size - 2;
        self.rows_remaining_in_page = u16::from_le_bytes([
            self.page_buf[row_count_idx],
            self.page_buf[row_count_idx + 1],
        ]) as usize;
        self.page_offset = 0;
        Ok(())
    }

    fn refill_batch<RDisk, WDisk>(
        &mut self,
        storage: &TempStorageManager,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<()>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        let (extent, page_offset_in_extent) =
            storage.locate_extent(self.file_id, self.current_page_index)?;

        let pages_left_in_extent = extent.num_pages - page_offset_in_extent;
        let pages_left_in_file = self.num_pages - self.current_page_index;
        let pages_to_read = pages_left_in_extent
            .min(pages_left_in_file)
            .min(self.io_batch_pages as u64);

        let bytes_to_read = (pages_to_read as usize)
            .checked_mul(self.block_size)
            .ok_or_else(|| anyhow!("batch read size overflow"))?;
        self.batch_buf.resize(bytes_to_read, 0);

        let start_page = TempPageId {
            file_id: self.file_id,
            page_index: self.current_page_index,
        };
        storage.read_pages(
            start_page,
            pages_to_read,
            &mut self.batch_buf,
            disk_reader,
            disk_writer,
        )?;

        self.batch_pages = pages_to_read;
        self.next_page_in_batch = 0;
        Ok(())
    }
}

fn get_anon_start_block<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
) -> Result<u64>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    disk_writer.write_all(b"get anon-start-block\n")?;
    disk_writer.flush()?;

    let mut line = String::new();
    disk_reader.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

fn get_blocks<RDisk, WDisk>(
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
    start_block_id: u64,
    num_blocks: u64,
    out: &mut [u8],
) -> Result<()>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    if num_blocks == 0 {
        if !out.is_empty() {
            bail!("non-empty output buffer provided for zero-block read");
        }
        return Ok(());
    }

    let cmd = format!("get block {} {}\n", start_block_id, num_blocks);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;
    std::io::Read::read_exact(disk_reader, out)?;
    Ok(())
}

fn put_blocks<WDisk>(
    disk_writer: &mut WDisk,
    start_block_id: u64,
    num_blocks: u64,
    data: &[u8],
) -> Result<()>
where
    WDisk: Write + ?Sized,
{
    let cmd = format!("put block {} {}\n", start_block_id, num_blocks);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.write_all(data)?;
    disk_writer.flush()?;
    Ok(())
}

fn encode_row(row: &Row) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    let field_count: u16 = row
        .len()
        .try_into()
        .map_err(|_| anyhow!("row has too many fields to serialize: {}", row.len()))?;
    out.extend_from_slice(&field_count.to_le_bytes());

    for value in row.values() {
        match value {
            Data::Int32(v) => {
                out.push(1);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Data::Int64(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Data::Float32(v) => {
                out.push(3);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Data::Float64(v) => {
                out.push(4);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Data::String(s) => {
                out.push(5);
                let bytes = s.as_bytes();
                let len: u32 = bytes
                    .len()
                    .try_into()
                    .map_err(|_| anyhow!("string too large to serialize: {} bytes", bytes.len()))?;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }

    Ok(out)
}

fn decode_row_from_page(page: &[u8], offset: &mut usize) -> Result<Row> {
    let usable_end = page.len() - 2;
    if *offset + 4 > usable_end {
        bail!(
            "decode overflow while reading row length: offset {}, usable_end {}",
            *offset,
            usable_end
        );
    }

    let row_len = u32::from_le_bytes([
        page[*offset],
        page[*offset + 1],
        page[*offset + 2],
        page[*offset + 3],
    ]) as usize;
    *offset += 4;

    if *offset + row_len > usable_end {
        bail!(
            "decode overflow while reading row payload: payload {}, offset {}, usable_end {}",
            row_len,
            *offset,
            usable_end
        );
    }

    let row_end = *offset + row_len;
    let mut cur = *offset;

    if cur + 2 > row_end {
        bail!("row payload too small to contain field count");
    }
    let field_count = u16::from_le_bytes([page[cur], page[cur + 1]]) as usize;
    cur += 2;

    let mut values = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        if cur >= row_end {
            bail!("unexpected end of row payload while reading field tag");
        }
        let tag = page[cur];
        cur += 1;

        let value = match tag {
            1 => {
                ensure_slice(cur, 4, row_end)?;
                let bytes: [u8; 4] = page[cur..cur + 4].try_into().unwrap();
                cur += 4;
                Data::Int32(i32::from_le_bytes(bytes))
            }
            2 => {
                ensure_slice(cur, 8, row_end)?;
                let bytes: [u8; 8] = page[cur..cur + 8].try_into().unwrap();
                cur += 8;
                Data::Int64(i64::from_le_bytes(bytes))
            }
            3 => {
                ensure_slice(cur, 4, row_end)?;
                let bytes: [u8; 4] = page[cur..cur + 4].try_into().unwrap();
                cur += 4;
                Data::Float32(f32::from_le_bytes(bytes))
            }
            4 => {
                ensure_slice(cur, 8, row_end)?;
                let bytes: [u8; 8] = page[cur..cur + 8].try_into().unwrap();
                cur += 8;
                Data::Float64(f64::from_le_bytes(bytes))
            }
            5 => {
                ensure_slice(cur, 4, row_end)?;
                let len = u32::from_le_bytes(page[cur..cur + 4].try_into().unwrap()) as usize;
                cur += 4;
                ensure_slice(cur, len, row_end)?;
                let s = std::str::from_utf8(&page[cur..cur + len])?.to_string();
                cur += len;
                Data::String(s)
            }
            other => bail!("unknown field tag {} while decoding row", other),
        };

        values.push(value);
    }

    if cur != row_end {
        bail!(
            "row payload not fully consumed: consumed {}, row_end {}",
            cur,
            row_end
        );
    }

    *offset = row_end;
    Ok(Row::new(values))
}

fn ensure_slice(start: usize, len: usize, end: usize) -> Result<()> {
    if start + len > end {
        bail!(
            "decode overflow: need {} bytes at offset {}, row_end {}",
            len,
            start,
            end
        );
    }
    Ok(())
}
