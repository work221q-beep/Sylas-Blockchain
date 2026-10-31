//! Sylas blockchain node.
//!
//! Rust-only implementation using:
//! - tokio for async HTTP and TCP networking
//! - rayon for parallel execution
//! - memmap2 for state storage
//! - sha2 for deterministic hashing

mod consensus;
mod error;
mod executor;
mod http;
mod ledger;
mod network;
mod p2p;
mod receipts;
mod scheduler;
mod storage;
mod types;
mod util;

use std::{env, path::PathBuf, sync::Arc, time::Duration};

use tokio::sync::{mpsc, Mutex};

use crate::{
    consensus::{consensus_config, ConsensusEngine},
    error::{EngineError, EngineResult},
    executor::{Engine, EngineConfig},
    http::{run_block_pipeline, run_http, AppState},
    ledger::LedgerStore,
    network::run_tcp_ingress,
    p2p::{bootstrap_peers, run_bootstrap_connector, run_p2p_service, PeerBook},
    receipts::ReceiptStore,
    storage::StateDb,
    types::{
        StateEntry, Transaction, MAX_TOKEN_SUPPLY_BASE_UNITS, TOKEN_DECIMALS, TOKEN_SYMBOL,
    },
    util::{decode_hex, hash_bytes, to_hex},
};

#[tokio::main]
async fn main() -> EngineResult<()> {
    let http_bind = env_string("SYLAS_HTTP_BIND", "0.0.0.0:5000");
    let tx_bind = env_string("SYLAS_TX_BIND", "0.0.0.0:7077");
    let p2p_bind = env_string("SYLAS_P2P_BIND", "0.0.0.0:9090");
    let configured_public_p2p_addr = env_string("SYLAS_PUBLIC_P2P_ADDR", "");
    let public_p2p_addr = if configured_public_p2p_addr.trim().is_empty() {
        p2p_bind.clone()
    } else {
        configured_public_p2p_addr
    };
    let configured_node_id = env_string("SYLAS_NODE_ID", "");
    let node_id = if configured_node_id.trim().is_empty() {
        derive_node_id(&http_bind, &tx_bind, &p2p_bind)
    } else {
        configured_node_id
    };
    let state_path = PathBuf::from(env_string("SYLAS_STATE_PATH", "/var/lib/sylas/state.mmap"));
    let ledger_path = PathBuf::from(env_string("SYLAS_LEDGER_PATH", "/var/lib/sylas/ledger.dat"));
    let receipts_path = PathBuf::from(env_string(
        "SYLAS_RECEIPTS_PATH",
        "/var/lib/sylas/receipts.log",
    ));
    let state_capacity = env_usize("SYLAS_STATE_CAPACITY", 65_536);
    let max_batch_size = env_usize("SYLAS_MAX_BATCH", 1_024);
    let genesis_accounts = env_string("SYLAS_GENESIS_ACCOUNTS", "");
    let token_symbol = TOKEN_SYMBOL.to_string();
    let token_decimals = TOKEN_DECIMALS;
    let token_price_usd = env_f64("SYLAS_TOKEN_PRICE_USD");
    let bootstrap_peers = bootstrap_peers(&env_string("SYLAS_BOOTSTRAP_PEERS", ""));
    let consensus_config = consensus_config(node_id.clone(), &env_string("SYLAS_VALIDATORS", ""));

    let db = StateDb::open(state_path, state_capacity)?;
    let ledger = LedgerStore::open(ledger_path)?;
    let receipts = ReceiptStore::open(receipts_path)?;
    let mut engine = Engine::new(
        db,
        ledger,
        receipts,
        EngineConfig {
            max_batch_size,
            ..EngineConfig::default()
        },
    )?;
    seed_genesis_accounts(&mut engine, &genesis_accounts)?;
    if engine.total_supply()? > MAX_TOKEN_SUPPLY_BASE_UNITS {
        return Err(EngineError::InvalidTransaction(
            "state supply exceeds 1,000,000 SYLAS cap",
        ));
    }

    let engine = Arc::new(Mutex::new(engine));
    let consensus = Arc::new(Mutex::new(ConsensusEngine::new(consensus_config)));
    let peer_book = Arc::new(Mutex::new(PeerBook::default()));
    let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(max_batch_size * 4);

    let tx_bind_for_ingress = tx_bind.clone();
    let tcp_sender = tx_sender.clone();
    tokio::spawn(async move {
        if let Err(err) = run_tcp_ingress(tx_bind_for_ingress, tcp_sender).await {
            eprintln!("transaction ingress stopped: {err}");
        }
    });
    let p2p_bind_for_service = p2p_bind.clone();
    let p2p_node_id = node_id.clone();
    let p2p_public_addr = public_p2p_addr.clone();
    let p2p_engine = engine.clone();
    let p2p_peer_book = peer_book.clone();
    tokio::spawn(async move {
        if let Err(err) = run_p2p_service(
            p2p_bind_for_service,
            p2p_node_id,
            p2p_public_addr,
            p2p_engine,
            p2p_peer_book,
        )
        .await
        {
            eprintln!("p2p service stopped: {err}");
        }
    });
    tokio::spawn(run_bootstrap_connector(
        bootstrap_peers,
        node_id.clone(),
        public_p2p_addr,
        engine.clone(),
        peer_book.clone(),
    ));
    tokio::spawn(run_block_pipeline(
        engine.clone(),
        consensus.clone(),
        tx_receiver,
        max_batch_size,
        Duration::from_millis(250),
    ));

    run_http(
        http_bind.clone(),
        AppState {
            engine,
            http_bind,
            tx_bind,
            p2p_bind,
            node_id,
            consensus,
            peer_book,
            tx_sender,
            token_symbol,
            token_decimals,
            token_price_usd,
        },
    )
    .await
}

fn seed_genesis_accounts(engine: &mut Engine, spec: &str) -> EngineResult<()> {
    if spec.trim().is_empty() {
        return Ok(());
    }
    let mut allocations = Vec::new();
    let mut projected_supply = engine.total_supply()?;
    for allocation in spec.split(',') {
        let mut parts = allocation.split(':');
        let key_hex = parts.next().unwrap_or("").trim();
        let balance_raw = parts.next().unwrap_or("").trim();
        if key_hex.is_empty() || balance_raw.is_empty() || parts.next().is_some() {
            return Err(EngineError::InvalidTransaction("invalid genesis account format"));
        }
        let key = decode_hex::<32>(key_hex, "genesis_account")?;
        let balance = balance_raw
            .parse::<u64>()
            .map_err(|_| EngineError::InvalidTransaction("invalid genesis balance"))?;
        let existing_balance = engine
            .account(&key)?
            .map_or(0, |account| account.balance);
        if balance > existing_balance {
            projected_supply = projected_supply
                .checked_add((balance - existing_balance) as u128)
                .ok_or(EngineError::ArithmeticOverflow)?;
        }
        if projected_supply > MAX_TOKEN_SUPPLY_BASE_UNITS {
            return Err(EngineError::InvalidTransaction(
                "genesis allocation exceeds 1,000,000 SYLAS cap",
            ));
        }
        allocations.push((key, balance));
    }
    for (key, balance) in allocations {
        let mut account = engine
            .state_db_mut()
            .get(&key)?
            .unwrap_or_else(|| StateEntry::empty_for(key));
        if account.balance < balance {
            account.balance = balance;
        }
        engine.state_db_mut().put(&account)?;
    }
    engine.state_db_mut().flush()
}

fn env_string(name: &str, fallback: &str) -> String {
    env::var(name).unwrap_or_else(|_| fallback.to_string())
}

fn derive_node_id(http_bind: &str, tx_bind: &str, p2p_bind: &str) -> String {
    let material = format!("{http_bind}|{tx_bind}|{p2p_bind}");
    let hash = to_hex(&hash_bytes(b"node-id", material.as_bytes()));
    format!("sylas-{}", &hash[..16])
}

fn env_usize(name: &str, fallback: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(fallback)
}

fn env_f64(name: &str) -> Option<f64> {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
}
