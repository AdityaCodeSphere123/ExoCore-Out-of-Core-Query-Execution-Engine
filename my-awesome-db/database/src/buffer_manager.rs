use anyhow::{anyhow, bail, Result};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, Write};

#[derive(Debug)]
struct Frame {
    block_id: u64,
    data: Vec<u8>,
}

pub struct BufferManager {
    block_size: usize,
    capacity: usize,
    frames: Vec<Option<Frame>>,
    page_table: HashMap<u64, usize>,
    fifo_queue: VecDeque<u64>,
    // Indices of frames that have never held a page (or were freed).
    // Popping here is O(1) vs the previous O(capacity) linear scan.
    free_frames: VecDeque<usize>,
}

impl BufferManager {
    pub fn new(block_size: usize, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            bail!("buffer manager capacity must be > 0");
        }

        let mut frames = Vec::with_capacity(capacity);
        frames.resize_with(capacity, || None);
        let free_frames = (0..capacity).collect();

        Ok(Self {
            block_size,
            capacity,
            frames,
            page_table: HashMap::new(),
            fifo_queue: VecDeque::new(),
            free_frames,
        })
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn contains(&self, block_id: u64) -> bool {
        self.page_table.contains_key(&block_id)
    }

    pub fn get_block<RDisk, WDisk>(
        &mut self,
        block_id: u64,
        disk_reader: &mut RDisk,
        disk_writer: &mut WDisk,
    ) -> Result<&[u8]>
    where
        RDisk: BufRead + ?Sized,
        WDisk: Write + ?Sized,
    {
        if let Some(&frame_idx) = self.page_table.get(&block_id) {
            let frame = self.frames[frame_idx]
                .as_ref()
                .ok_or_else(|| anyhow!("page table pointed to empty frame"))?;
            return Ok(&frame.data);
        }

        let new_data = fetch_block_from_disk(block_id, self.block_size, disk_reader, disk_writer)?;

        let frame_idx = if let Some(idx) = self.find_free_frame() {
            idx
        } else {
            self.evict_one()?
        };

        self.frames[frame_idx] = Some(Frame {
            block_id,
            data: new_data,
        });
        self.page_table.insert(block_id, frame_idx);
        self.fifo_queue.push_back(block_id);

        let frame = self.frames[frame_idx]
            .as_ref()
            .ok_or_else(|| anyhow!("newly inserted frame missing"))?;
        Ok(&frame.data)
    }

    fn find_free_frame(&mut self) -> Option<usize> {
        self.free_frames.pop_front()
    }

    fn evict_one(&mut self) -> Result<usize> {
        let victim_block_id = self
            .fifo_queue
            .pop_front()
            .ok_or_else(|| anyhow!("no victim available for eviction"))?;

        let frame_idx = self
            .page_table
            .remove(&victim_block_id)
            .ok_or_else(|| anyhow!("victim block missing from page table"))?;

        self.frames[frame_idx] = None;
        Ok(frame_idx)
    }
}

fn fetch_block_from_disk<RDisk, WDisk>(
    block_id: u64,
    block_size: usize,
    disk_reader: &mut RDisk,
    disk_writer: &mut WDisk,
) -> Result<Vec<u8>>
where
    RDisk: BufRead + ?Sized,
    WDisk: Write + ?Sized,
{
    let cmd = format!("get block {} 1\n", block_id);
    disk_writer.write_all(cmd.as_bytes())?;
    disk_writer.flush()?;

    let mut buf = vec![0u8; block_size];
    std::io::Read::read_exact(disk_reader, &mut buf)?;
    Ok(buf)
}