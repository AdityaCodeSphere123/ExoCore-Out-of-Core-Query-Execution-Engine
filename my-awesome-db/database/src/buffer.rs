use anyhow::{bail, Result};

pub struct BlockBuffer<'a> {
    data: &'a [u8],
}

impl<'a> BlockBuffer<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data
    }

    pub fn usable_end(&self) -> Result<usize> {
        if self.data.len() < 2 {
            bail!("block too small to contain row count");
        }
        Ok(self.data.len() - 2)
    }

    pub fn row_count(&self) -> Result<usize> {
        if self.data.len() < 2 {
            bail!("block too small to contain row count");
        }
        let idx = self.data.len() - 2;
        Ok(u16::from_le_bytes([self.data[idx], self.data[idx + 1]]) as usize)
    }

    pub fn ensure_bytes(&self, offset: usize, needed: usize) -> Result<()> {
        let usable_end = self.usable_end()?;
        if offset + needed > usable_end {
            bail!(
                "decode overflow: need {} bytes at offset {}, usable_end {}",
                needed,
                offset,
                usable_end
            );
        }
        Ok(())
    }

    pub fn read_i32(&self, offset: &mut usize) -> Result<i32> {
        self.ensure_bytes(*offset, 4)?;
        let bytes: [u8; 4] = self.data[*offset..*offset + 4].try_into().unwrap();
        *offset += 4;
        Ok(i32::from_le_bytes(bytes))
    }

    pub fn read_i64(&self, offset: &mut usize) -> Result<i64> {
        self.ensure_bytes(*offset, 8)?;
        let bytes: [u8; 8] = self.data[*offset..*offset + 8].try_into().unwrap();
        *offset += 8;
        Ok(i64::from_le_bytes(bytes))
    }

    pub fn read_f32(&self, offset: &mut usize) -> Result<f32> {
        self.ensure_bytes(*offset, 4)?;
        let bytes: [u8; 4] = self.data[*offset..*offset + 4].try_into().unwrap();
        *offset += 4;
        Ok(f32::from_le_bytes(bytes))
    }

    pub fn read_f64(&self, offset: &mut usize) -> Result<f64> {
        self.ensure_bytes(*offset, 8)?;
        let bytes: [u8; 8] = self.data[*offset..*offset + 8].try_into().unwrap();
        *offset += 8;
        Ok(f64::from_le_bytes(bytes))
    }

    pub fn read_cstring(&self, offset: &mut usize) -> Result<String> {
        let usable_end = self.usable_end()?;
        let start = *offset;

        while *offset < usable_end && self.data[*offset] != 0 {
            *offset += 1;
        }

        if *offset >= usable_end {
            bail!("unterminated string while decoding row");
        }

        let s = std::str::from_utf8(&self.data[start..*offset])?.to_string();
        *offset += 1;
        Ok(s)
    }
}