//! Append-only block ledger.
//!
//! Account state is memory-mapped, but a blockchain also needs a durable chain
//! head. This store persists block headers so a restarted node recovers height,
//! previous hash, and recent block history instead of pairing old state with a
//! fresh genesis head.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    error::{EngineError, EngineResult},
    types::Block,
};

const MAGIC: &[u8; 8] = b"SYLASLG1";
const LEDGER_VERSION: u64 = 1;
const HEADER_SIZE: u64 = 64;
const RECORD_SIZE: u64 = 160;

pub struct LedgerStore {
    file: File,
}

impl LedgerStore {
    pub fn open(path: impl AsRef<Path>) -> EngineResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if file.metadata()?.len() == 0 {
            initialize_header(&mut file)?;
        } else {
            validate_header(&mut file)?;
            validate_record_layout(&file)?;
        }
        Ok(Self { file })
    }

    pub fn append_block(&mut self, block: &Block) -> EngineResult<()> {
        self.file.seek(SeekFrom::End(0))?;
        let mut record = Vec::with_capacity(RECORD_SIZE as usize);
        write_u64(&mut record, block.height);
        record.extend_from_slice(&block.previous_hash);
        record.extend_from_slice(&block.state_root);
        record.extend_from_slice(&block.tx_root);
        write_u64(&mut record, block.tx_count as u64);
        write_u128(&mut record, block.timestamp_ms);
        record.extend_from_slice(&block.hash);
        debug_assert_eq!(record.len(), RECORD_SIZE as usize);
        self.file.write_all(&record)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn last_block(&mut self) -> EngineResult<Option<Block>> {
        let count = self.record_count()?;
        if count == 0 {
            return Ok(None);
        }
        self.read_block(count - 1).map(Some)
    }

    pub fn recent_blocks(&mut self, limit: usize) -> EngineResult<Vec<Block>> {
        let count = self.record_count()?;
        let take = (limit as u64).min(count);
        let mut blocks = Vec::with_capacity(take as usize);
        for index in count - take..count {
            blocks.push(self.read_block(index)?);
        }
        Ok(blocks)
    }

    fn record_count(&self) -> EngineResult<u64> {
        let len = self.file.metadata()?.len();
        if len < HEADER_SIZE {
            return Err(EngineError::StorageCorruption("ledger file too small"));
        }
        let payload = len - HEADER_SIZE;
        if payload % RECORD_SIZE != 0 {
            return Err(EngineError::StorageCorruption("ledger record layout mismatch"));
        }
        Ok(payload / RECORD_SIZE)
    }

    fn read_block(&mut self, index: u64) -> EngineResult<Block> {
        let mut record = [0u8; RECORD_SIZE as usize];
        self.file
            .seek(SeekFrom::Start(HEADER_SIZE + index * RECORD_SIZE))?;
        self.file.read_exact(&mut record)?;

        let height = read_u64(&record, 0);
        let mut previous_hash = [0u8; 32];
        previous_hash.copy_from_slice(&record[8..40]);
        let mut state_root = [0u8; 32];
        state_root.copy_from_slice(&record[40..72]);
        let mut tx_root = [0u8; 32];
        tx_root.copy_from_slice(&record[72..104]);
        let tx_count = read_u64(&record, 104) as usize;
        let timestamp_ms = read_u128(&record, 112);
        let mut stored_hash = [0u8; 32];
        stored_hash.copy_from_slice(&record[128..160]);

        let block = Block::new(
            height,
            previous_hash,
            state_root,
            tx_root,
            tx_count,
            timestamp_ms,
        );
        if block.hash != stored_hash {
            return Err(EngineError::StorageCorruption("ledger block hash mismatch"));
        }
        Ok(block)
    }
}

fn initialize_header(file: &mut File) -> EngineResult<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; HEADER_SIZE as usize];
    header[0..MAGIC.len()].copy_from_slice(MAGIC);
    header[8..16].copy_from_slice(&LEDGER_VERSION.to_le_bytes());
    file.write_all(&header)?;
    file.sync_data()?;
    Ok(())
}

fn validate_header(file: &mut File) -> EngineResult<()> {
    let mut header = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    if &header[0..MAGIC.len()] != MAGIC {
        return Err(EngineError::StorageCorruption("bad ledger magic"));
    }
    if read_u64(&header, 8) != LEDGER_VERSION {
        return Err(EngineError::StorageCorruption("unsupported ledger version"));
    }
    Ok(())
}

fn validate_record_layout(file: &File) -> EngineResult<()> {
    let len = file.metadata()?.len();
    if len < HEADER_SIZE || (len - HEADER_SIZE) % RECORD_SIZE != 0 {
        return Err(EngineError::StorageCorruption("ledger record layout mismatch"));
    }
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}

fn read_u128(bytes: &[u8], offset: usize) -> u128 {
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes[offset..offset + 16]);
    u128::from_le_bytes(out)
}
