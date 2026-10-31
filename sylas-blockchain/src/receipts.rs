//! Durable transaction receipt index.
//!
//! Blocks and state are already persisted by `ledger.rs` and `storage.rs`.
//! This append-only log keeps explorer receipts available after a container
//! restart or redeploy as long as `/var/lib/sylas` is mounted persistently.

use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    error::{EngineError, EngineResult},
    executor::TransactionRecord,
    types::{Pubkey, TransferOp},
    util::{decode_hex, decode_hex_vec, to_hex, Hash},
};

const RECEIPT_VERSION: &str = "TXR1";

pub struct ReceiptStore {
    path: PathBuf,
}

impl ReceiptStore {
    pub fn open(path: impl AsRef<Path>) -> EngineResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        Ok(Self { path })
    }

    pub fn append(&self, record: &TransactionRecord) -> EngineResult<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        file.write_all(encode_record(record).as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    pub fn load_recent(&self, limit: usize) -> EngineResult<Vec<TransactionRecord>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        let mut records = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| decode_record(line).ok())
            .collect::<Vec<_>>();
        if records.len() > limit {
            let remove_count = records.len() - limit;
            records.drain(..remove_count);
        }
        Ok(records)
    }
}

fn encode_record(record: &TransactionRecord) -> String {
    [
        RECEIPT_VERSION.to_string(),
        to_hex(&record.tx_id),
        record.block_height.to_string(),
        to_hex(&record.block_hash),
        record.timestamp_ms.to_string(),
        record.kind.to_string(),
        if record.success { "1" } else { "0" }.to_string(),
        record.stage.to_string(),
        to_hex(&record.signer),
        record.fee.to_string(),
        to_hex(&record.recent_block_hash),
        encode_pubkeys(&record.read_set),
        encode_pubkeys(&record.write_set),
        encode_transfers(&record.transfers),
        to_hex(record.message.as_bytes()),
    ]
    .join("|")
}

fn decode_record(line: &str) -> EngineResult<TransactionRecord> {
    let parts = line.split('|').collect::<Vec<_>>();
    if parts.len() != 15 || parts[0] != RECEIPT_VERSION {
        return Err(EngineError::StorageCorruption("bad receipt record"));
    }
    Ok(TransactionRecord {
        tx_id: parse_hash(parts[1], "receipt.tx_id")?,
        block_height: parse_u64(parts[2], "receipt block height")?,
        block_hash: parse_hash(parts[3], "receipt.block_hash")?,
        timestamp_ms: parse_u128(parts[4], "receipt timestamp")?,
        kind: parse_kind(parts[5]),
        success: parts[6] == "1",
        message: parse_message(parts[14])?,
        stage: parse_usize(parts[7], "receipt stage")?,
        signer: parse_pubkey(parts[8], "receipt.signer")?,
        fee: parse_u64(parts[9], "receipt fee")?,
        recent_block_hash: parse_hash(parts[10], "receipt.recent_block_hash")?,
        read_set: parse_pubkeys(parts[11])?,
        write_set: parse_pubkeys(parts[12])?,
        transfers: parse_transfers(parts[13])?,
    })
}

fn encode_pubkeys(keys: &[Pubkey]) -> String {
    keys.iter().map(|key| to_hex(key)).collect::<Vec<_>>().join(",")
}

fn parse_pubkeys(value: &str) -> EngineResult<Vec<Pubkey>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| parse_pubkey(part, "receipt.pubkey"))
        .collect()
}

fn encode_transfers(transfers: &[TransferOp]) -> String {
    transfers
        .iter()
        .map(|transfer| {
            format!(
                "{}:{}:{}",
                to_hex(&transfer.from),
                to_hex(&transfer.to),
                transfer.amount
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_transfers(value: &str) -> EngineResult<Vec<TransferOp>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| {
            let fields = part.split(':').collect::<Vec<_>>();
            if fields.len() != 3 {
                return Err(EngineError::StorageCorruption("bad transfer receipt"));
            }
            Ok(TransferOp {
                from: parse_pubkey(fields[0], "receipt.transfer.from")?,
                to: parse_pubkey(fields[1], "receipt.transfer.to")?,
                amount: parse_u64(fields[2], "receipt transfer amount")?,
            })
        })
        .collect()
}

fn parse_message(value: &str) -> EngineResult<String> {
    let bytes = decode_hex_vec(value)?;
    String::from_utf8(bytes).map_err(|_| EngineError::StorageCorruption("bad receipt message"))
}

fn parse_kind(value: &str) -> &'static str {
    match value {
        "airdrop" => "airdrop",
        _ => "transfer",
    }
}

fn parse_hash(value: &str, field: &'static str) -> EngineResult<Hash> {
    decode_hex::<32>(value, field)
}

fn parse_pubkey(value: &str, field: &'static str) -> EngineResult<Pubkey> {
    decode_hex::<32>(value, field)
}

fn parse_u64(value: &str, reason: &'static str) -> EngineResult<u64> {
    value
        .parse::<u64>()
        .map_err(|_| EngineError::StorageCorruption(reason))
}

fn parse_u128(value: &str, reason: &'static str) -> EngineResult<u128> {
    value
        .parse::<u128>()
        .map_err(|_| EngineError::StorageCorruption(reason))
}

fn parse_usize(value: &str, reason: &'static str) -> EngineResult<usize> {
    value
        .parse::<usize>()
        .map_err(|_| EngineError::StorageCorruption(reason))
}
