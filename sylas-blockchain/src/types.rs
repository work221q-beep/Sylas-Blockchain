//! Core blockchain data structures.
//!
//! Transactions declare their account access up front. The scheduler never
//! infers account locks from opaque program code; it uses these declared sets
//! to build deterministic parallel execution stages.

use std::{collections::HashSet, fmt};

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::{
    error::{EngineError, EngineResult},
    util::{Hash, hash_chunks, to_hex, write_u32, write_u64},
};

pub type Pubkey = [u8; 32];

pub const MAX_ACCOUNTS_PER_TX: usize = 64;
pub const MAX_TRANSFERS_PER_TX: usize = 32;
pub const CHAIN_ID: &[u8] = b"sylas-mainnet";
pub const TOKEN_SYMBOL: &str = "SYLAS";
pub const TOKEN_DECIMALS: u8 = 8;
pub const MAX_TOKEN_SUPPLY_NATIVE: u64 = 1_000_000;
pub const MAX_TOKEN_SUPPLY_BASE_UNITS: u128 = 100_000_000_000_000;
pub const AIRDROP_AMOUNT_BASE_UNITS: u64 = 100_000_000;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AccountId(pub Pubkey);

impl AccountId {
    pub fn to_hex(self) -> String {
        to_hex(&self.0)
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Variable-width account view used by the execution layer.
#[derive(Clone, Debug)]
pub struct Account {
    pub key: Pubkey,
    pub balance: u64,
    pub nonce: u64,
    pub version: u64,
    pub data: Vec<u8>,
}

/// A fixed-width account record stored in the memory-mapped state file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateEntry {
    pub key: Pubkey,
    pub balance: u64,
    pub nonce: u64,
    pub version: u64,
    pub data: [u8; StateEntry::DATA_BYTES],
    pub data_len: u32,
}

impl StateEntry {
    pub const DATA_BYTES: usize = 128;

    pub fn empty_for(key: Pubkey) -> Self {
        Self {
            key,
            balance: 0,
            nonce: 0,
            version: 0,
            data: [0u8; Self::DATA_BYTES],
            data_len: 0,
        }
    }

    pub fn account(&self) -> Account {
        Account {
            key: self.key,
            balance: self.balance,
            nonce: self.nonce,
            version: self.version,
            data: self.data[..self.data_len as usize].to_vec(),
        }
    }

    pub fn hash(&self) -> Hash {
        hash_chunks(
            b"state-entry",
            &[
                &self.key,
                &self.balance.to_le_bytes(),
                &self.nonce.to_le_bytes(),
                &self.version.to_le_bytes(),
                &self.data_len.to_le_bytes(),
                &self.data[..self.data_len as usize],
            ],
        )
    }
}

/// A simple native-balance transfer operation.
///
/// The minimal engine intentionally keeps execution semantics small. A
/// production smart-contract VM would replace this operation list with program
/// instructions, but the scheduler and storage design would stay the same.
#[derive(Clone, Debug)]
pub struct TransferOp {
    pub from: Pubkey,
    pub to: Pubkey,
    pub amount: u64,
}

/// A signed transaction with explicit account access declarations.
#[derive(Clone, Debug)]
pub struct Transaction {
    pub signer: Pubkey,
    pub signature: [u8; 64],
    pub nonce: u64,
    pub fee: u64,
    pub recent_block_hash: Hash,
    pub read_set: Vec<Pubkey>,
    pub write_set: Vec<Pubkey>,
    pub transfers: Vec<TransferOp>,
}

impl Transaction {
    pub fn unsigned(
        signer: Pubkey,
        nonce: u64,
        fee: u64,
        recent_block_hash: Hash,
        read_set: Vec<Pubkey>,
        write_set: Vec<Pubkey>,
        transfers: Vec<TransferOp>,
    ) -> EngineResult<Self> {
        let tx = Self {
            signer,
            signature: [0u8; 64],
            nonce,
            fee,
            recent_block_hash,
            read_set,
            write_set,
            transfers,
        };
        tx.validate_access_sets()?;
        Ok(tx)
    }

    pub fn signed(
        signing_key: &SigningKey,
        nonce: u64,
        fee: u64,
        recent_block_hash: Hash,
        read_set: Vec<Pubkey>,
        write_set: Vec<Pubkey>,
        transfers: Vec<TransferOp>,
    ) -> EngineResult<Self> {
        let signer = signing_key.verifying_key().to_bytes();
        let mut tx = Self::unsigned(
            signer,
            nonce,
            fee,
            recent_block_hash,
            read_set,
            write_set,
            transfers,
        )?;
        let signature = signing_key.sign(&tx.signing_payload());
        tx.signature = signature.to_bytes();
        Ok(tx)
    }

    pub fn id(&self) -> Hash {
        hash_chunks(b"transaction-id", &[&self.signing_payload(), &self.signature])
    }

    pub fn verify(&self) -> EngineResult<()> {
        self.validate_access_sets()?;
        let verifying_key = VerifyingKey::from_bytes(&self.signer)
            .map_err(|_| EngineError::InvalidTransaction("bad signer public key"))?;
        let signature = Signature::from_bytes(&self.signature);
        verifying_key
            .verify(&self.signing_payload(), &signature)
            .map_err(|_| EngineError::InvalidSignature)
    }

    pub fn touches_write(&self, account: &Pubkey) -> bool {
        self.write_set.iter().any(|candidate| candidate == account)
    }

    /// Deterministic binary signing payload.
    ///
    /// The signature binds every field that affects scheduling or state
    /// transition, including declared read/write sets.
    pub fn signing_payload(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256 + self.read_set.len() * 32 + self.write_set.len() * 32);
        out.extend_from_slice(b"sylas-tx-v1");
        write_u32(&mut out, CHAIN_ID.len() as u32);
        out.extend_from_slice(CHAIN_ID);
        out.extend_from_slice(&self.signer);
        write_u64(&mut out, self.nonce);
        write_u64(&mut out, self.fee);
        out.extend_from_slice(&self.recent_block_hash);

        write_u32(&mut out, self.read_set.len() as u32);
        for key in &self.read_set {
            out.extend_from_slice(key);
        }
        write_u32(&mut out, self.write_set.len() as u32);
        for key in &self.write_set {
            out.extend_from_slice(key);
        }
        write_u32(&mut out, self.transfers.len() as u32);
        for transfer in &self.transfers {
            out.extend_from_slice(&transfer.from);
            out.extend_from_slice(&transfer.to);
            write_u64(&mut out, transfer.amount);
        }
        out
    }

    fn validate_access_sets(&self) -> EngineResult<()> {
        if self.write_set.is_empty() {
            return Err(EngineError::InvalidTransaction("write_set cannot be empty"));
        }
        if self.read_set.len() > MAX_ACCOUNTS_PER_TX || self.write_set.len() > MAX_ACCOUNTS_PER_TX {
            return Err(EngineError::InvalidTransaction("too many accounts in access set"));
        }
        if self.transfers.is_empty() || self.transfers.len() > MAX_TRANSFERS_PER_TX {
            return Err(EngineError::InvalidTransaction("invalid transfer count"));
        }
        ensure_unique(&self.read_set, "read_set")?;
        ensure_unique(&self.write_set, "write_set")?;
        if !self.touches_write(&self.signer) {
            return Err(EngineError::InvalidTransaction("signer must be writable to pay fees"));
        }
        for transfer in &self.transfers {
            if transfer.amount == 0 {
                return Err(EngineError::InvalidTransaction("transfer amount must be non-zero"));
            }
            if transfer.from != self.signer {
                return Err(EngineError::InvalidTransaction(
                    "minimal engine only allows signer-funded transfers",
                ));
            }
            if !self.touches_write(&transfer.from) || !self.touches_write(&transfer.to) {
                return Err(EngineError::InvalidTransaction(
                    "transfer accounts must be in write_set",
                ));
            }
        }
        Ok(())
    }
}

fn ensure_unique(keys: &[Pubkey], name: &'static str) -> EngineResult<()> {
    let mut seen = HashSet::with_capacity(keys.len());
    for key in keys {
        if !seen.insert(*key) {
            return Err(EngineError::InvalidTransaction(match name {
                "read_set" => "duplicate account in read_set",
                "write_set" => "duplicate account in write_set",
                _ => "duplicate account",
            }));
        }
    }
    Ok(())
}

/// Executed block summary.
#[derive(Clone, Debug)]
pub struct Block {
    pub height: u64,
    pub previous_hash: Hash,
    pub state_root: Hash,
    pub tx_root: Hash,
    pub tx_count: usize,
    pub timestamp_ms: u128,
    pub hash: Hash,
}

impl Block {
    pub fn new(
        height: u64,
        previous_hash: Hash,
        state_root: Hash,
        tx_root: Hash,
        tx_count: usize,
        timestamp_ms: u128,
    ) -> Self {
        let hash = hash_chunks(
            b"block",
            &[
                &height.to_le_bytes(),
                &previous_hash,
                &state_root,
                &tx_root,
                &(tx_count as u64).to_le_bytes(),
                &timestamp_ms.to_le_bytes(),
            ],
        );
        Self {
            height,
            previous_hash,
            state_root,
            tx_root,
            tx_count,
            timestamp_ms,
            hash,
        }
    }
}
