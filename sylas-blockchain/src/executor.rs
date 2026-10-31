//! Parallel transaction executor.
//!
//! The executor is deliberately deterministic:
//!
//! 1. Verify signatures in parallel.
//! 2. Build a write-conflict plan.
//! 3. For each stage, snapshot all declared read/write accounts.
//! 4. Execute transactions in that stage concurrently.
//! 5. Commit successful writes in original transaction order.

use std::{
    collections::{BTreeSet, HashMap},
    time::{SystemTime, UNIX_EPOCH},
};

use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

use crate::{
    error::{EngineError, EngineResult},
    ledger::LedgerStore,
    receipts::ReceiptStore,
    scheduler::Scheduler,
    storage::StateDb,
    types::{Block, Pubkey, StateEntry, Transaction, TransferOp},
    util::{Hash, hash_bytes, hash_chunks},
};

const AIRDROP_COOLDOWN_MS: u128 = 86_400_000;
const AIRDROP_TAG: &[u8; 8] = b"AIRDROP1";
const TRANSACTION_HISTORY_LIMIT: usize = 10_000;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub execution_threads: usize,
    pub max_batch_size: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        let available = std::thread::available_parallelism().map_or(2, usize::from);
        Self {
            execution_threads: available.clamp(2, 8),
            max_batch_size: 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransactionReceipt {
    pub tx_id: Hash,
    pub success: bool,
    pub message: String,
    pub stage: usize,
}

#[derive(Clone, Debug)]
pub struct BlockExecution {
    pub block: Block,
    pub receipts: Vec<TransactionReceipt>,
    pub verified_transactions: usize,
    pub rejected_transactions: usize,
    pub conflict_edges: usize,
    pub stages: usize,
}

#[derive(Clone, Debug)]
pub struct AirdropReceipt {
    pub account: Pubkey,
    pub amount: u64,
    pub next_available_ms: u128,
    pub block: Block,
}

#[derive(Clone, Debug)]
pub struct TransactionRecord {
    pub tx_id: Hash,
    pub block_height: u64,
    pub block_hash: Hash,
    pub timestamp_ms: u128,
    pub kind: &'static str,
    pub success: bool,
    pub message: String,
    pub stage: usize,
    pub signer: Pubkey,
    pub fee: u64,
    pub recent_block_hash: Hash,
    pub read_set: Vec<Pubkey>,
    pub write_set: Vec<Pubkey>,
    pub transfers: Vec<TransferOp>,
}

impl TransactionRecord {
    fn from_transaction(tx: &Transaction, receipt: &TransactionReceipt, block: &Block) -> Self {
        Self {
            tx_id: receipt.tx_id,
            block_height: block.height,
            block_hash: block.hash,
            timestamp_ms: block.timestamp_ms,
            kind: "transfer",
            success: receipt.success,
            message: receipt.message.clone(),
            stage: receipt.stage,
            signer: tx.signer,
            fee: tx.fee,
            recent_block_hash: tx.recent_block_hash,
            read_set: tx.read_set.clone(),
            write_set: tx.write_set.clone(),
            transfers: tx.transfers.clone(),
        }
    }

    fn airdrop(account: Pubkey, amount: u64, block: &Block) -> Self {
        let tx_id = hash_chunks(
            b"airdrop-record",
            &[&account, &amount.to_le_bytes(), &block.hash],
        );
        Self {
            tx_id,
            block_height: block.height,
            block_hash: block.hash,
            timestamp_ms: block.timestamp_ms,
            kind: "airdrop",
            success: true,
            message: "committed".to_string(),
            stage: 0,
            signer: account,
            fee: 0,
            recent_block_hash: block.previous_hash,
            read_set: Vec::new(),
            write_set: vec![account],
            transfers: vec![TransferOp {
                from: [0u8; 32],
                to: account,
                amount,
            }],
        }
    }

    pub fn touches(&self, account: &Pubkey) -> bool {
        self.signer == *account
            || self.read_set.iter().any(|key| key == account)
            || self.write_set.iter().any(|key| key == account)
            || self
                .transfers
                .iter()
                .any(|transfer| transfer.from == *account || transfer.to == *account)
    }
}

pub struct Engine {
    db: StateDb,
    ledger: LedgerStore,
    pool: ThreadPool,
    config: EngineConfig,
    max_batch_size: usize,
    height: u64,
    previous_hash: Hash,
    blocks: Vec<Block>,
    last_execution: Option<BlockExecution>,
    receipt_store: ReceiptStore,
    transactions: Vec<TransactionRecord>,
}

impl Engine {
    pub fn new(
        db: StateDb,
        mut ledger: LedgerStore,
        receipt_store: ReceiptStore,
        config: EngineConfig,
    ) -> EngineResult<Self> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(config.execution_threads)
            .thread_name(|index| format!("sylas-exec-{index}"))
            .build()
            .map_err(|err| EngineError::SchedulerBuild(err.to_string()))?;
        let recent_blocks = ledger.recent_blocks(50)?;
        let latest_block = recent_blocks.last().cloned();
        let height = latest_block.as_ref().map_or(0, |block| block.height);
        let previous_hash = latest_block
            .as_ref()
            .map_or_else(|| hash_bytes(b"genesis", b"sylas"), |block| block.hash);
        if let Some(block) = &latest_block {
            let current_state_root = db.state_root()?;
            if current_state_root != block.state_root {
                return Err(EngineError::StorageCorruption(
                    "state root does not match ledger head",
                ));
            }
        }
        let transactions = receipt_store.load_recent(TRANSACTION_HISTORY_LIMIT)?;
        Ok(Self {
            db,
            ledger,
            pool,
            config: config.clone(),
            max_batch_size: config.max_batch_size,
            height,
            previous_hash,
            blocks: recent_blocks,
            last_execution: None,
            receipt_store,
            transactions,
        })
    }

    pub fn state_db_mut(&mut self) -> &mut StateDb {
        &mut self.db
    }

    pub fn account_count(&self) -> usize {
        self.db.len()
    }

    pub fn state_capacity(&self) -> usize {
        self.db.capacity()
    }

    pub fn state_root(&self) -> EngineResult<Hash> {
        self.db.state_root()
    }

    pub fn account(&self, key: &Pubkey) -> EngineResult<Option<StateEntry>> {
        self.db.get(key)
    }

    pub fn accounts(&self, limit: usize) -> EngineResult<Vec<StateEntry>> {
        let mut accounts = self.db.accounts()?;
        accounts.sort_by(|left, right| {
            right
                .balance
                .cmp(&left.balance)
                .then_with(|| left.key.cmp(&right.key))
        });
        accounts.truncate(limit);
        Ok(accounts)
    }

    pub fn total_supply(&self) -> EngineResult<u128> {
        self.db.total_supply()
    }

    pub fn height(&self) -> u64 {
        self.height
    }

    pub fn execution_threads(&self) -> usize {
        self.config.execution_threads
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub fn recent_blocks(&self, limit: usize) -> Vec<Block> {
        let start = self.blocks.len().saturating_sub(limit);
        self.blocks[start..].iter().rev().cloned().collect()
    }

    pub fn last_execution(&self) -> Option<BlockExecution> {
        self.last_execution.clone()
    }

    pub fn recent_transactions(&self, limit: usize) -> Vec<TransactionRecord> {
        let start = self.transactions.len().saturating_sub(limit);
        self.transactions[start..].iter().rev().cloned().collect()
    }

    pub fn transaction(&self, tx_id: &Hash) -> Option<TransactionRecord> {
        self.transactions
            .iter()
            .rev()
            .find(|record| &record.tx_id == tx_id)
            .cloned()
    }

    pub fn account_transactions(
        &self,
        account: &Pubkey,
        limit: usize,
    ) -> Vec<TransactionRecord> {
        self.transactions
            .iter()
            .rev()
            .filter(|record| record.touches(account))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn recent_block_hash(&self) -> Hash {
        self.previous_hash
    }

    pub fn execute_block(&mut self, transactions: Vec<Transaction>) -> EngineResult<BlockExecution> {
        if transactions.len() > self.max_batch_size {
            return Err(EngineError::InvalidTransaction("batch exceeds max_batch_size"));
        }
        let expected_recent_block_hash = self.previous_hash;
        let verification_results: Vec<(Transaction, EngineResult<()>)> = self.pool.install(|| {
            transactions
                .into_par_iter()
                .map(|tx| {
                    let result = if tx.recent_block_hash != expected_recent_block_hash {
                        Err(EngineError::InvalidTransaction("recent block hash expired"))
                    } else {
                        tx.verify()
                    };
                    (tx, result)
                })
                .collect()
        });
        let mut verified = Vec::new();
        let mut rejected_receipts = Vec::new();
        for (tx, result) in verification_results {
            match result {
                Ok(()) => verified.push(tx),
                Err(err) => {
                    let tx_id = tx.id();
                    rejected_receipts.push((
                        tx,
                        TransactionReceipt {
                            tx_id,
                            success: false,
                            message: err.to_string(),
                            stage: 0,
                        },
                    ));
                }
            }
        }
        let rejected_transactions = rejected_receipts.len();
        let plan = if verified.is_empty() {
            crate::scheduler::ExecutionPlan {
                stages: Vec::new(),
                conflict_edges: 0,
            }
        } else {
            Scheduler::plan(&verified)
        };
        let mut receipt_entries = Vec::with_capacity(verified.len());

        for (stage_index, stage) in plan.stages.iter().enumerate() {
            let snapshot_keys = collect_stage_keys(stage, &verified);
            let snapshots = self.db.snapshot_accounts(snapshot_keys)?;
            let mut stage_results: Vec<ExecutionOutcome> = self.pool.install(|| {
                stage
                    .par_iter()
                    .map(|tx_index| execute_transaction(*tx_index, &verified[*tx_index], &snapshots))
                    .collect()
            });
            stage_results.sort_by_key(|result| result.tx_index);
            for result in stage_results {
                let tx_id = verified[result.tx_index].id();
                match result.result {
                    Ok(updates) => {
                        for update in updates {
                            self.db.put(&update)?;
                        }
                        receipt_entries.push((
                            result.tx_index,
                            TransactionReceipt {
                                tx_id,
                                success: true,
                                message: "committed".to_string(),
                                stage: stage_index,
                            },
                        ));
                    }
                    Err(err) => receipt_entries.push((
                        result.tx_index,
                        TransactionReceipt {
                            tx_id,
                            success: false,
                            message: err.to_string(),
                            stage: stage_index,
                        },
                    )),
                }
            }
        }

        self.db.flush()?;
        let state_root = self.db.state_root()?;
        let tx_root = transaction_root(
            rejected_receipts
                .iter()
                .map(|(_, receipt)| receipt.tx_id)
                .chain(receipt_entries.iter().map(|(_, receipt)| receipt.tx_id)),
        );
        let committed_count = rejected_receipts
            .iter()
            .map(|(_, receipt)| receipt)
            .chain(receipt_entries.iter().map(|(_, receipt)| receipt))
            .filter(|receipt| receipt.success)
            .count();
        let block = Block::new(
            self.height + 1,
            self.previous_hash,
            state_root,
            tx_root,
            committed_count,
            now_ms(),
        );
        self.ledger.append_block(&block)?;
        self.height = block.height;
        self.previous_hash = block.hash;
        for (tx, receipt) in &rejected_receipts {
            let record = TransactionRecord::from_transaction(tx, receipt, &block);
            self.receipt_store.append(&record)?;
            self.transactions.push(record);
        }
        for (tx_index, receipt) in &receipt_entries {
            let record = TransactionRecord::from_transaction(&verified[*tx_index], receipt, &block);
            self.receipt_store.append(&record)?;
            self.transactions.push(record);
        }
        trim_transaction_history(&mut self.transactions);
        let mut receipts = rejected_receipts
            .into_iter()
            .map(|(_, receipt)| receipt)
            .collect::<Vec<_>>();
        receipts.extend(receipt_entries
            .into_iter()
            .map(|(_, receipt)| receipt)
            .collect::<Vec<_>>());
        let execution = BlockExecution {
            block,
            receipts,
            verified_transactions: verified.len(),
            rejected_transactions,
            conflict_edges: plan.conflict_edges,
            stages: plan.stages.len(),
        };
        self.blocks.push(execution.block.clone());
        self.last_execution = Some(execution.clone());
        Ok(execution)
    }

    pub fn request_airdrop(
        &mut self,
        account: Pubkey,
        amount: u64,
        max_supply: u128,
    ) -> EngineResult<AirdropReceipt> {
        let now = now_ms();
        let mut entry = self.db.get(&account)?.unwrap_or_else(|| StateEntry::empty_for(account));
        let last_airdrop_ms = read_airdrop_ms(&entry);
        if last_airdrop_ms != 0 && now.saturating_sub(last_airdrop_ms) < AIRDROP_COOLDOWN_MS {
            return Err(EngineError::InvalidTransaction("airdrop cooldown active"));
        }
        let next_supply = self
            .total_supply()?
            .checked_add(amount as u128)
            .ok_or(EngineError::ArithmeticOverflow)?;
        if next_supply > max_supply {
            return Err(EngineError::InvalidTransaction("airdrop would exceed token cap"));
        }
        entry.balance = entry
            .balance
            .checked_add(amount)
            .ok_or(EngineError::ArithmeticOverflow)?;
        entry.version = entry
            .version
            .checked_add(1)
            .ok_or(EngineError::ArithmeticOverflow)?;
        write_airdrop_ms(&mut entry, now);
        self.db.put(&entry)?;
        self.db.flush()?;

        let state_root = self.db.state_root()?;
        let tx_root = hash_chunks(b"airdrop", &[&account, &amount.to_le_bytes(), &now.to_le_bytes()]);
        let block = Block::new(
            self.height + 1,
            self.previous_hash,
            state_root,
            tx_root,
            1,
            now,
        );
        self.ledger.append_block(&block)?;
        self.height = block.height;
        self.previous_hash = block.hash;
        self.blocks.push(block.clone());
        let record = TransactionRecord::airdrop(account, amount, &block);
        self.receipt_store.append(&record)?;
        self.transactions.push(record);
        trim_transaction_history(&mut self.transactions);
        Ok(AirdropReceipt {
            account,
            amount,
            next_available_ms: now + AIRDROP_COOLDOWN_MS,
            block,
        })
    }

    pub fn recommended_fee(&self) -> u64 {
        let Some(execution) = &self.last_execution else {
            return 5_000;
        };
        let congestion = execution
            .conflict_edges
            .saturating_mul(250)
            .saturating_add(execution.stages.saturating_sub(1).saturating_mul(1_000))
            .saturating_add(execution.verified_transactions / 10);
        (5_000u64).saturating_add(congestion as u64).min(50_000)
    }
}

struct ExecutionOutcome {
    tx_index: usize,
    result: EngineResult<Vec<StateEntry>>,
}

fn execute_transaction(
    tx_index: usize,
    tx: &Transaction,
    snapshots: &HashMap<Pubkey, StateEntry>,
) -> ExecutionOutcome {
    let result = apply_transaction(tx, snapshots);
    ExecutionOutcome { tx_index, result }
}

fn apply_transaction(
    tx: &Transaction,
    snapshots: &HashMap<Pubkey, StateEntry>,
) -> EngineResult<Vec<StateEntry>> {
    let mut writable = HashMap::with_capacity(tx.write_set.len());
    for key in &tx.write_set {
        let entry = snapshots
            .get(key)
            .cloned()
            .unwrap_or_else(|| StateEntry::empty_for(*key));
        writable.insert(*key, entry);
    }

    let signer = writable
        .get(&tx.signer)
        .ok_or(EngineError::InvalidTransaction("signer account not writable"))?;
    let expected_nonce = signer
        .nonce
        .checked_add(1)
        .ok_or(EngineError::ArithmeticOverflow)?;
    if tx.nonce != expected_nonce {
        return Err(EngineError::InvalidTransaction("nonce mismatch"));
    }

    debit(&mut writable, tx.signer, tx.fee)?;
    for transfer in &tx.transfers {
        debit(&mut writable, transfer.from, transfer.amount)?;
        credit(&mut writable, transfer.to, transfer.amount)?;
    }
    writable
        .get_mut(&tx.signer)
        .ok_or(EngineError::InvalidTransaction("signer account not writable"))?
        .nonce = tx.nonce;

    let mut updates = Vec::with_capacity(writable.len());
    for key in &tx.write_set {
        let mut entry = writable
            .remove(key)
            .ok_or(EngineError::InvalidTransaction("missing writable account"))?;
        entry.version = entry
            .version
            .checked_add(1)
            .ok_or(EngineError::ArithmeticOverflow)?;
        updates.push(entry);
    }
    Ok(updates)
}

fn debit(accounts: &mut HashMap<Pubkey, StateEntry>, key: Pubkey, amount: u64) -> EngineResult<()> {
    if amount == 0 {
        return Ok(());
    }
    let account = accounts
        .get_mut(&key)
        .ok_or(EngineError::InvalidTransaction("debit account not writable"))?;
    if account.balance < amount {
        return Err(EngineError::InsufficientFunds);
    }
    account.balance -= amount;
    Ok(())
}

fn credit(accounts: &mut HashMap<Pubkey, StateEntry>, key: Pubkey, amount: u64) -> EngineResult<()> {
    if amount == 0 {
        return Ok(());
    }
    let account = accounts
        .get_mut(&key)
        .ok_or(EngineError::InvalidTransaction("credit account not writable"))?;
    account.balance = account
        .balance
        .checked_add(amount)
        .ok_or(EngineError::ArithmeticOverflow)?;
    Ok(())
}

fn collect_stage_keys(stage: &[usize], transactions: &[Transaction]) -> Vec<Pubkey> {
    let mut keys = BTreeSet::new();
    for index in stage {
        let tx = &transactions[*index];
        keys.extend(tx.read_set.iter().copied());
        keys.extend(tx.write_set.iter().copied());
    }
    keys.into_iter().collect()
}

fn transaction_root(ids: impl Iterator<Item = Hash>) -> Hash {
    let mut level: Vec<Hash> = ids.collect();
    if level.is_empty() {
        return hash_bytes(b"tx-root-empty", b"");
    }
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        for pair in level.chunks(2) {
            let left = pair[0];
            let right = if pair.len() == 2 { pair[1] } else { left };
            next.push(hash_chunks(b"tx-root-node", &[&left, &right]));
        }
        level = next;
    }
    level[0]
}

fn trim_transaction_history(transactions: &mut Vec<TransactionRecord>) {
    if transactions.len() > TRANSACTION_HISTORY_LIMIT {
        let remove_count = transactions.len() - TRANSACTION_HISTORY_LIMIT;
        transactions.drain(..remove_count);
    }
}

fn read_airdrop_ms(entry: &StateEntry) -> u128 {
    if entry.data_len as usize >= AIRDROP_TAG.len() + 16
        && &entry.data[..AIRDROP_TAG.len()] == AIRDROP_TAG
    {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&entry.data[AIRDROP_TAG.len()..AIRDROP_TAG.len() + 16]);
        u128::from_le_bytes(bytes)
    } else {
        0
    }
}

fn write_airdrop_ms(entry: &mut StateEntry, value: u128) {
    entry.data = [0u8; StateEntry::DATA_BYTES];
    entry.data[..AIRDROP_TAG.len()].copy_from_slice(AIRDROP_TAG);
    entry.data[AIRDROP_TAG.len()..AIRDROP_TAG.len() + 16].copy_from_slice(&value.to_le_bytes());
    entry.data_len = (AIRDROP_TAG.len() + 16) as u32;
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}
