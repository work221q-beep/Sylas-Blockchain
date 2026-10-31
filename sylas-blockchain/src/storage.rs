//! Memory-mapped state storage.
//!
//! The database is deliberately simple: fixed-width slots addressed by
//! open-addressed hashing. This makes reads/writes predictable, keeps memory
//! pressure low, and lets the OS page cache do the heavy lifting on small VPS
//! deployments.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    path::Path,
};

use memmap2::MmapMut;

use crate::{
    error::{EngineError, EngineResult},
    types::{Pubkey, StateEntry},
    util::{Hash, hash_bytes, hash_chunks},
};

const MAGIC: &[u8; 8] = b"SYLASDB1";
const STORAGE_VERSION: u64 = 1;
const HEADER_SIZE: usize = 4096;
const SLOT_SIZE: usize = 256;
const OCCUPIED_OFFSET: usize = 0;
const KEY_OFFSET: usize = 1;
const BALANCE_OFFSET: usize = 33;
const VERSION_OFFSET: usize = 41;
const NONCE_OFFSET: usize = 49;
const DATA_LEN_OFFSET: usize = 57;
const DATA_OFFSET: usize = 61;

pub struct StateDb {
    file: File,
    mmap: MmapMut,
    capacity: usize,
}

impl StateDb {
    /// Opens or creates a memory-mapped account database.
    ///
    /// `capacity` is the maximum number of account slots. With the default slot
    /// size, `65536` accounts uses about 16 MB on disk and in the virtual memory
    /// map, while actual resident memory is controlled by the OS page cache.
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> EngineResult<Self> {
        if capacity == 0 {
            return Err(EngineError::StorageCorruption("capacity cannot be zero"));
        }
        let path = path.as_ref();
        let existed = path.exists();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = HEADER_SIZE + capacity * SLOT_SIZE;
        file.set_len(file_len as u64)?;

        // SAFETY: The file is opened read/write, sized before mapping, and this
        // StateDb owns the mutable mapping. All access to bytes goes through
        // bounds-checked slices and explicit little-endian encoding.
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        if existed && &mmap[0..MAGIC.len()] == MAGIC {
            Self::validate_header(&mmap, capacity)?;
        } else {
            Self::initialize_header(&mut mmap, capacity);
            mmap.flush()?;
        }

        Ok(Self {
            file,
            mmap,
            capacity,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        read_u64(&self.mmap, 24) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, key: &Pubkey) -> EngineResult<Option<StateEntry>> {
        match self.find_slot(key)? {
            Some(slot) => self.read_slot(slot).map(Some),
            None => Ok(None),
        }
    }

    pub fn get_or_empty(&self, key: Pubkey) -> EngineResult<StateEntry> {
        Ok(self.get(&key)?.unwrap_or_else(|| StateEntry::empty_for(key)))
    }

    pub fn put(&mut self, entry: &StateEntry) -> EngineResult<()> {
        if entry.data_len as usize > StateEntry::DATA_BYTES {
            return Err(EngineError::InvalidTransaction("account data too large"));
        }
        let (slot, existed) = self.find_slot_or_empty(&entry.key)?;
        self.write_slot(slot, entry);
        if !existed {
            let next_len = self
                .len()
                .checked_add(1)
                .ok_or(EngineError::ArithmeticOverflow)?;
            write_u64(&mut self.mmap, 24, next_len as u64);
        }
        Ok(())
    }

    pub fn snapshot_accounts(&self, keys: impl IntoIterator<Item = Pubkey>) -> EngineResult<HashMap<Pubkey, StateEntry>> {
        let mut out = HashMap::new();
        for key in keys {
            out.entry(key).or_insert(self.get_or_empty(key)?);
        }
        Ok(out)
    }

    pub fn accounts(&self) -> EngineResult<Vec<StateEntry>> {
        let mut accounts = Vec::with_capacity(self.len());
        for slot in 0..self.capacity {
            if self.slot_occupied(slot) {
                accounts.push(self.read_slot(slot)?);
            }
        }
        Ok(accounts)
    }

    pub fn total_supply(&self) -> EngineResult<u128> {
        let mut supply = 0u128;
        for account in self.accounts()? {
            supply = supply
                .checked_add(account.balance as u128)
                .ok_or(EngineError::ArithmeticOverflow)?;
        }
        Ok(supply)
    }

    pub fn state_root(&self) -> EngineResult<Hash> {
        let mut leaves = Vec::with_capacity(self.len());
        for slot in 0..self.capacity {
            if self.slot_occupied(slot) {
                let entry = self.read_slot(slot)?;
                leaves.push((entry.key, entry.hash()));
            }
        }
        leaves.sort_by(|a, b| a.0.cmp(&b.0));
        let mut level: Vec<Hash> = leaves
            .into_iter()
            .map(|(key, leaf_hash)| hash_chunks(b"state-leaf", &[&key, &leaf_hash]))
            .collect();
        if level.is_empty() {
            return Ok(hash_bytes(b"state-empty", b""));
        }
        while level.len() > 1 {
            let mut next = Vec::with_capacity(level.len().div_ceil(2));
            for pair in level.chunks(2) {
                let left = pair[0];
                let right = if pair.len() == 2 { pair[1] } else { left };
                next.push(hash_chunks(b"state-node", &[&left, &right]));
            }
            level = next;
        }
        Ok(level[0])
    }

    pub fn flush(&mut self) -> EngineResult<()> {
        self.mmap.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    fn initialize_header(mmap: &mut [u8], capacity: usize) {
        mmap.fill(0);
        mmap[0..MAGIC.len()].copy_from_slice(MAGIC);
        write_u64(mmap, 8, STORAGE_VERSION);
        write_u64(mmap, 16, capacity as u64);
        write_u64(mmap, 24, 0);
    }

    fn validate_header(mmap: &[u8], capacity: usize) -> EngineResult<()> {
        if &mmap[0..MAGIC.len()] != MAGIC {
            return Err(EngineError::StorageCorruption("bad magic"));
        }
        if read_u64(mmap, 8) != STORAGE_VERSION {
            return Err(EngineError::StorageCorruption("unsupported storage version"));
        }
        let stored_capacity = read_u64(mmap, 16) as usize;
        if stored_capacity != capacity {
            return Err(EngineError::StorageCorruption(
                "capacity mismatch; reopen with original capacity",
            ));
        }
        Ok(())
    }

    fn find_slot(&self, key: &Pubkey) -> EngineResult<Option<usize>> {
        let start = self.slot_index(key);
        for probe in 0..self.capacity {
            let slot = (start + probe) % self.capacity;
            if !self.slot_occupied(slot) {
                return Ok(None);
            }
            if self.slot_key(slot) == *key {
                return Ok(Some(slot));
            }
        }
        Ok(None)
    }

    fn find_slot_or_empty(&self, key: &Pubkey) -> EngineResult<(usize, bool)> {
        let start = self.slot_index(key);
        for probe in 0..self.capacity {
            let slot = (start + probe) % self.capacity;
            if !self.slot_occupied(slot) {
                return Ok((slot, false));
            }
            if self.slot_key(slot) == *key {
                return Ok((slot, true));
            }
        }
        Err(EngineError::StateFull)
    }

    fn slot_index(&self, key: &Pubkey) -> usize {
        let digest = hash_bytes(b"state-slot", key);
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest[..8]);
        (u64::from_le_bytes(bytes) as usize) % self.capacity
    }

    fn slot_offset(slot: usize) -> usize {
        HEADER_SIZE + slot * SLOT_SIZE
    }

    fn slot_occupied(&self, slot: usize) -> bool {
        self.mmap[Self::slot_offset(slot) + OCCUPIED_OFFSET] == 1
    }

    fn slot_key(&self, slot: usize) -> Pubkey {
        let offset = Self::slot_offset(slot) + KEY_OFFSET;
        let mut key = [0u8; 32];
        key.copy_from_slice(&self.mmap[offset..offset + 32]);
        key
    }

    fn read_slot(&self, slot: usize) -> EngineResult<StateEntry> {
        let offset = Self::slot_offset(slot);
        if self.mmap[offset + OCCUPIED_OFFSET] != 1 {
            return Err(EngineError::AccountNotFound);
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&self.mmap[offset + KEY_OFFSET..offset + KEY_OFFSET + 32]);
        let balance = read_u64(&self.mmap, offset + BALANCE_OFFSET);
        let version = read_u64(&self.mmap, offset + VERSION_OFFSET);
        let nonce = read_u64(&self.mmap, offset + NONCE_OFFSET);
        let data_len = read_u32(&self.mmap, offset + DATA_LEN_OFFSET);
        if data_len as usize > StateEntry::DATA_BYTES {
            return Err(EngineError::StorageCorruption("account data length out of range"));
        }
        let mut data = [0u8; StateEntry::DATA_BYTES];
        data.copy_from_slice(
            &self.mmap[offset + DATA_OFFSET..offset + DATA_OFFSET + StateEntry::DATA_BYTES],
        );
        Ok(StateEntry {
            key,
            balance,
            nonce,
            version,
            data,
            data_len,
        })
    }

    fn write_slot(&mut self, slot: usize, entry: &StateEntry) {
        let offset = Self::slot_offset(slot);
        self.mmap[offset + OCCUPIED_OFFSET] = 1;
        self.mmap[offset + KEY_OFFSET..offset + KEY_OFFSET + 32].copy_from_slice(&entry.key);
        write_u64(&mut self.mmap, offset + BALANCE_OFFSET, entry.balance);
        write_u64(&mut self.mmap, offset + VERSION_OFFSET, entry.version);
        write_u64(&mut self.mmap, offset + NONCE_OFFSET, entry.nonce);
        write_u32(&mut self.mmap, offset + DATA_LEN_OFFSET, entry.data_len);
        self.mmap[offset + DATA_OFFSET..offset + DATA_OFFSET + StateEntry::DATA_BYTES]
            .copy_from_slice(&entry.data);
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut out = [0u8; 4];
    out.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(out)
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
