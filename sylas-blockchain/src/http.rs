//! Tokio HTTP server for the Sylas console and public node API.
//!
//! The node avoids a web framework to keep the deployment small. Routes are
//! bounded, explicit, and served directly over `tokio::net::TcpListener`.

use std::{sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};

use crate::{
    consensus::{snapshot_json, validator_json, ConsensusEngine},
    error::{EngineError, EngineResult},
    executor::{Engine, TransactionRecord},
    network::parse_transaction_line,
    p2p::{peers_json as peer_list_json, SharedPeerBook},
    types::{
        Block, StateEntry, Transaction, AIRDROP_AMOUNT_BASE_UNITS, CHAIN_ID,
        MAX_TOKEN_SUPPLY_BASE_UNITS, MAX_TOKEN_SUPPLY_NATIVE,
    },
    util::{decode_hex, to_hex},
};

const MAX_HTTP_REQUEST_BYTES: usize = 64 * 1024;
const ACCOUNT_LIST_LIMIT: usize = 100;
const TRANSACTION_LIST_LIMIT: usize = 100;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Mutex<Engine>>,
    pub http_bind: String,
    pub tx_bind: String,
    pub p2p_bind: String,
    pub node_id: String,
    pub consensus: Arc<Mutex<ConsensusEngine>>,
    pub peer_book: SharedPeerBook,
    pub tx_sender: mpsc::Sender<Transaction>,
    pub token_symbol: String,
    pub token_decimals: u8,
    pub token_price_usd: Option<f64>,
}

struct HttpRequest {
    method: String,
    path: String,
    body: String,
}

pub async fn run_http(bind: String, state: AppState) -> EngineResult<()> {
    let listener = TcpListener::bind(&bind).await?;
    println!("Sylas HTTP dashboard listening on {bind}");
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, state).await {
                eprintln!("http connection error: {err}");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, state: AppState) -> EngineResult<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };
    let method = request.method.as_str();
    let path = request.path.as_str();

    match (method, path) {
        ("GET", "/") => write_response(&mut stream, 200, "text/html; charset=utf-8", INDEX_HTML).await,
        ("GET", "/explorer") | ("GET", "/explorer/") => {
            write_response(&mut stream, 200, "text/html; charset=utf-8", EXPLORER_HTML).await
        },
        ("GET", "/healthz") => write_response(&mut stream, 200, "text/plain; charset=utf-8", "ok\n").await,
        ("GET", "/api/status") => write_json_or_error(&mut stream, 200, 500, status_json(&state).await).await,
        ("GET", "/api/debug") => {
            write_json_or_error(&mut stream, 200, 500, debug_json(&state).await).await
        },
        ("GET", "/api/consensus") => {
            write_json_or_error(&mut stream, 200, 500, consensus_json(&state).await).await
        },
        ("GET", "/api/validators") => {
            write_response(
                &mut stream,
                200,
                "application/json; charset=utf-8",
                &validators_json(&state).await,
            )
            .await
        },
        ("GET", "/api/peers") => {
            write_response(
                &mut stream,
                200,
                "application/json; charset=utf-8",
                &peers_json(&state).await,
            )
            .await
        },
        ("GET", "/api/blocks") => write_response(
            &mut stream,
            200,
            "application/json; charset=utf-8",
            &blocks_json(&state).await,
        )
        .await,
        ("GET", "/api/transactions") => write_response(
            &mut stream,
            200,
            "application/json; charset=utf-8",
            &transactions_json(&state).await,
        )
        .await,
        ("GET", "/api/economics") => {
            write_json_or_error(&mut stream, 200, 500, economics_json(&state).await).await
        },
        ("GET", "/api/fees") => write_response(
            &mut stream,
            200,
            "application/json; charset=utf-8",
            &fees_json(&state).await,
        )
        .await,
        ("GET", "/api/accounts") => {
            write_json_or_error(&mut stream, 200, 500, accounts_json(&state).await).await
        },
        ("POST", "/api/airdrop") => match request_airdrop(&state, &request.body).await {
            Ok(body) => write_response(&mut stream, 202, "application/json; charset=utf-8", &body).await,
            Err(err) => {
                let body = error_json(&err.to_string());
                write_response(&mut stream, 400, "application/json; charset=utf-8", &body).await
            }
        },
        ("POST", "/api/tokens/create")
        | ("POST", "/api/tokens/close-account")
        | ("POST", "/api/swap/quote")
        | ("POST", "/api/bridge/quote") => {
            let body = reserved_endpoint_json(path);
            write_response(&mut stream, 501, "application/json; charset=utf-8", &body).await
        },
        ("POST", "/api/transactions") => match submit_transaction(&state, &request.body).await {
            Ok(body) => write_response(&mut stream, 202, "application/json; charset=utf-8", &body).await,
            Err(err) => {
                let body = error_json(&err.to_string());
                write_response(&mut stream, 400, "application/json; charset=utf-8", &body).await
            }
        },
        _ if method == "GET" && path.starts_with("/api/transaction/") => {
            let tx_id_hex = path.trim_start_matches("/api/transaction/");
            write_json_or_error(&mut stream, 200, 404, transaction_json(&state, tx_id_hex).await).await
        },
        _ if method == "GET" && path.starts_with("/api/account/") && path.ends_with("/transactions") => {
            let key_hex = path
                .trim_start_matches("/api/account/")
                .trim_end_matches("/transactions")
                .trim_end_matches('/');
            write_json_or_error(&mut stream, 200, 400, account_transactions_json(&state, key_hex).await).await
        },
        _ if method == "GET" && path.starts_with("/api/account/") => {
            let key_hex = path.trim_start_matches("/api/account/");
            write_json_or_error(&mut stream, 200, 400, account_json(&state, key_hex).await).await
        },
        _ => write_response(&mut stream, 404, "text/plain; charset=utf-8", "not found\n").await,
    }
}

async fn read_request(stream: &mut TcpStream) -> EngineResult<Option<HttpRequest>> {
    let mut bytes = Vec::with_capacity(4096);
    let mut buffer = [0u8; 4096];

    let header_end = loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(EngineError::Network("incomplete http request".into()))
            };
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > MAX_HTTP_REQUEST_BYTES {
            return Err(EngineError::Network("http request too large".into()));
        }
        if let Some(index) = find_header_end(&bytes) {
            break index;
        }
    };

    let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
    let content_length = parse_content_length(&headers)?;
    let request_length = header_end + 4 + content_length;
    if request_length > MAX_HTTP_REQUEST_BYTES {
        return Err(EngineError::Network("http request too large".into()));
    }
    while bytes.len() < request_length {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(EngineError::Network("incomplete http body".into()));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > MAX_HTTP_REQUEST_BYTES {
            return Err(EngineError::Network("http request too large".into()));
        }
    }

    let Some(first_line) = headers.lines().next() else {
        return Err(EngineError::Network("missing http request line".into()));
    };
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let raw_path = parts.next().unwrap_or("/").to_string();
    let path = raw_path.split('?').next().unwrap_or("/").to_string();
    let body = String::from_utf8_lossy(&bytes[header_end + 4..request_length]).to_string();

    Ok(Some(HttpRequest { method, path, body }))
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == &b"\r\n\r\n"[..])
}

fn parse_content_length(headers: &str) -> EngineResult<usize> {
    for line in headers.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<usize>()
                .map_err(|_| EngineError::Network("bad content-length".into()));
        }
    }
    Ok(0)
}

async fn write_json_or_error(
    stream: &mut TcpStream,
    status: u16,
    error_status: u16,
    result: EngineResult<String>,
) -> EngineResult<()> {
    match result {
        Ok(body) => write_response(stream, status, "application/json; charset=utf-8", &body).await,
        Err(err) => {
            let body = error_json(&err.to_string());
            write_response(stream, error_status, "application/json; charset=utf-8", &body).await
        }
    }
}

async fn status_json(state: &AppState) -> EngineResult<String> {
    let engine = state.engine.lock().await;
    let state_root = engine.state_root()?;
    let total_supply = engine.total_supply()?;
    let recent_block_hash = engine.recent_block_hash();
    let peer_count = state.peer_book.lock().await.len();
    let consensus_snapshot = state
        .consensus
        .lock()
        .await
        .snapshot(recent_block_hash, state_root);
    let consensus = snapshot_json(&consensus_snapshot);
    let last_execution = engine.last_execution();
    let (last_verified, last_rejected, last_stages, last_conflicts) =
        last_execution.map_or((0, 0, 0, 0), |execution| {
            (
                execution.verified_transactions,
                execution.rejected_transactions,
                execution.stages,
                execution.conflict_edges,
            )
        });
    let market_cap = market_cap_json(total_supply, state.token_decimals, state.token_price_usd);
    let price = optional_f64_json(state.token_price_usd);
    Ok(format!(
        r#"{{
  "node": "sylas",
  "chain_id": "{}",
  "status": "online",
  "engine": "rust-tokio-rayon-memmap2-sha2-ed25519",
  "ledger": "append-only-block-headers",
  "height": {},
  "accounts": {},
  "state_capacity": {},
  "execution_threads": {},
  "max_batch_size": {},
  "http_bind": "{}",
  "tx_bind": "{}",
  "p2p_bind": "{}",
  "state_root": "{}",
  "recent_block_hash": "{}",
  "network": {{
    "node_id": "{}",
    "p2p_bind": "{}",
    "peers": {}
  }},
  "consensus": {},
  "economics": {{
    "token_symbol": "{}",
    "token_decimals": {},
    "max_supply_native": {},
    "max_supply_base_units": "{}",
    "total_supply_base_units": "{}",
    "price_usd": {},
    "market_cap_usd": {}
  }},
  "last_batch": {{
    "verified": {},
    "rejected": {},
    "stages": {},
    "write_conflicts": {}
  }}
}}"#,
        String::from_utf8_lossy(CHAIN_ID),
        engine.height(),
        engine.account_count(),
        engine.state_capacity(),
        engine.execution_threads(),
        engine.max_batch_size(),
        json_escape(&state.http_bind),
        json_escape(&state.tx_bind),
        json_escape(&state.p2p_bind),
        to_hex(&state_root),
        to_hex(&recent_block_hash),
        json_escape(&state.node_id),
        json_escape(&state.p2p_bind),
        peer_count,
        consensus,
        json_escape(&state.token_symbol),
        state.token_decimals,
        MAX_TOKEN_SUPPLY_NATIVE,
        MAX_TOKEN_SUPPLY_BASE_UNITS,
        total_supply,
        price,
        market_cap,
        last_verified,
        last_rejected,
        last_stages,
        last_conflicts
    ))
}

async fn debug_json(state: &AppState) -> EngineResult<String> {
    let engine = state.engine.lock().await;
    let state_root = engine.state_root()?;
    let height = engine.height();
    let recent_block_hash = engine.recent_block_hash();
    let account_count = engine.account_count();
    let state_capacity = engine.state_capacity();
    let execution_threads = engine.execution_threads();
    let max_batch_size = engine.max_batch_size();
    let total_supply = engine.total_supply()?;
    let latest_block = engine.recent_blocks(1).into_iter().next();
    let ledger_consistent = if height == 0 {
        true
    } else {
        latest_block.as_ref().is_some_and(|block| {
            block.height == height
                && block.hash == recent_block_hash
                && block.state_root == state_root
        })
    };
    let latest_block_detail = latest_block.map_or_else(
        || "genesis head; no committed blocks yet".to_string(),
        |block| {
            format!(
                "height={} txs={} hash={}",
                block.height,
                block.tx_count,
                to_hex(&block.hash)
            )
        },
    );
    drop(engine);

    let peers = state.peer_book.lock().await.len();
    let consensus_snapshot = state
        .consensus
        .lock()
        .await
        .snapshot(recent_block_hash, state_root);

    let mut checks = Vec::new();
    checks.push(debug_check(
        "state.storage",
        "pass",
        &format!(
            "state root {} across {} / {} account slots",
            to_hex(&state_root),
            account_count,
            state_capacity
        ),
    ));
    checks.push(debug_check(
        "state.capacity",
        if account_count <= state_capacity { "pass" } else { "fail" },
        &format!("{account_count} accounts stored in {state_capacity} slots"),
    ));
    checks.push(debug_check(
        "ledger.head",
        if ledger_consistent { "pass" } else { "fail" },
        &latest_block_detail,
    ));
    checks.push(debug_check(
        "execution.threads",
        if execution_threads > 0 { "pass" } else { "fail" },
        &format!("{execution_threads} Rayon worker threads configured"),
    ));
    checks.push(debug_check(
        "execution.batch",
        if max_batch_size > 0 { "pass" } else { "fail" },
        &format!("{max_batch_size} transaction batch limit"),
    ));
    checks.push(debug_check(
        "transaction.pipeline",
        if state.tx_sender.is_closed() { "fail" } else { "pass" },
        if state.tx_sender.is_closed() {
            "transaction queue is closed"
        } else {
            "transaction queue is accepting signed transactions"
        },
    ));
    checks.push(debug_check(
        "consensus.validators",
        if consensus_snapshot.validator_count > 0
            && consensus_snapshot.total_power >= consensus_snapshot.quorum_power
        {
            "pass"
        } else {
            "fail"
        },
        &format!(
            "{} validators, total_power={}, quorum_power={}",
            consensus_snapshot.validator_count,
            consensus_snapshot.total_power,
            consensus_snapshot.quorum_power
        ),
    ));
    checks.push(debug_check(
        "consensus.mode",
        if consensus_snapshot.validator_count > 1 {
            "pass"
        } else {
            "warn"
        },
        consensus_snapshot.mode,
    ));
    checks.push(debug_check(
        "network.p2p",
        if state.p2p_bind.trim().is_empty() { "fail" } else { "pass" },
        &format!("p2p bind {}", state.p2p_bind),
    ));
    checks.push(debug_check(
        "network.peers",
        if peers > 0 { "pass" } else { "warn" },
        &format!("{peers} peers currently discovered"),
    ));
    checks.push(debug_check(
        "market.price",
        if state.token_price_usd.is_some() { "pass" } else { "warn" },
        if state.token_price_usd.is_some() {
            "external token price configured"
        } else {
            "market cap unavailable until external price is configured"
        },
    ));
    checks.push(debug_check(
        "token.supply_cap",
        if total_supply <= MAX_TOKEN_SUPPLY_BASE_UNITS {
            "pass"
        } else {
            "fail"
        },
        &format!(
            "supply={} cap={} base units",
            total_supply, MAX_TOKEN_SUPPLY_BASE_UNITS
        ),
    ));
    checks.push(debug_check(
        "token.decimals",
        if state.token_decimals <= 18 { "pass" } else { "warn" },
        &format!("{} display decimals", state.token_decimals),
    ));

    let fail_count = checks.iter().filter(|check| check.status == "fail").count();
    let warn_count = checks.iter().filter(|check| check.status == "warn").count();
    let check_json = checks
        .iter()
        .map(DebugCheck::json)
        .collect::<Vec<String>>()
        .join(",");

    Ok(format!(
        r#"{{
  "status": "{}",
  "failures": {},
  "warnings": {},
  "checks": [{}]
}}"#,
        if fail_count > 0 {
            "fail"
        } else if warn_count > 0 {
            "warn"
        } else {
            "pass"
        },
        fail_count,
        warn_count,
        check_json
    ))
}

async fn consensus_json(state: &AppState) -> EngineResult<String> {
    let engine = state.engine.lock().await;
    let state_root = engine.state_root()?;
    let recent_block_hash = engine.recent_block_hash();
    let snapshot = state
        .consensus
        .lock()
        .await
        .snapshot(recent_block_hash, state_root);
    Ok(snapshot_json(&snapshot))
}

async fn validators_json(state: &AppState) -> String {
    let consensus = state.consensus.lock().await;
    validator_json(consensus.validators())
}

async fn peers_json(state: &AppState) -> String {
    let peers = state.peer_book.lock().await.list();
    peer_list_json(&peers)
}

async fn fees_json(state: &AppState) -> String {
    let engine = state.engine.lock().await;
    let recommended_fee = engine.recommended_fee();
    let last_execution = engine.last_execution();
    let (stages, conflicts, verified) = last_execution.map_or((0, 0, 0), |execution| {
        (
            execution.stages,
            execution.conflict_edges,
            execution.verified_transactions,
        )
    });
    format!(
        r#"{{"base_fee":5000,"recommended_fee":{},"max_fee":50000,"unit":"base_units","congestion":{{"stages":{},"write_conflicts":{},"verified_transactions":{}}}}}"#,
        recommended_fee, stages, conflicts, verified
    )
}

async fn economics_json(state: &AppState) -> EngineResult<String> {
    let engine = state.engine.lock().await;
    let total_supply = engine.total_supply()?;
    Ok(format!(
        r#"{{
  "token_symbol": "{}",
  "token_decimals": {},
  "max_supply_native": {},
  "max_supply_base_units": "{}",
  "total_supply_base_units": "{}",
  "price_usd": {},
  "market_cap_usd": {},
  "market_data": "{}"
}}"#,
        json_escape(&state.token_symbol),
        state.token_decimals,
        MAX_TOKEN_SUPPLY_NATIVE,
        MAX_TOKEN_SUPPLY_BASE_UNITS,
        total_supply,
        optional_f64_json(state.token_price_usd),
        market_cap_json(total_supply, state.token_decimals, state.token_price_usd),
        if state.token_price_usd.is_some() {
            "configured"
        } else {
            "external_price_required"
        }
    ))
}

async fn request_airdrop(state: &AppState, body: &str) -> EngineResult<String> {
    let account = parse_airdrop_account(body)?;
    let receipt = {
        let mut engine = state.engine.lock().await;
        engine.request_airdrop(account, AIRDROP_AMOUNT_BASE_UNITS, MAX_TOKEN_SUPPLY_BASE_UNITS)?
    };
    state.consensus.lock().await.record_commit(&receipt.block);
    Ok(format!(
        r#"{{"ok":true,"account":"{}","amount_base_units":"{}","amount_native":"1","next_available_ms":{},"block":{}}}"#,
        to_hex(&receipt.account),
        receipt.amount,
        receipt.next_available_ms,
        block_json(&receipt.block)
    ))
}

fn parse_airdrop_account(body: &str) -> EngineResult<[u8; 32]> {
    let trimmed = body.trim();
    if trimmed.starts_with('{') {
        if let Some(start) = trimmed.find("\"account\"") {
            let after_key = &trimmed[start + "\"account\"".len()..];
            if let Some(colon) = after_key.find(':') {
                let after_colon = after_key[colon + 1..].trim_start();
                if let Some(rest) = after_colon.strip_prefix('"') {
                    if let Some(end) = rest.find('"') {
                        return decode_hex::<32>(&rest[..end], "airdrop.account");
                    }
                }
            }
        }
        return Err(EngineError::InvalidTransaction("airdrop account missing"));
    }
    decode_hex::<32>(trimmed, "airdrop.account")
}

fn reserved_endpoint_json(path: &str) -> String {
    let feature = match path {
        "/api/tokens/create" => "secondary_token_create",
        "/api/tokens/close-account" => "secondary_token_close_account",
        "/api/swap/quote" => "swap_quote",
        "/api/bridge/quote" => "bridge_quote",
        _ => "reserved",
    };
    format!(
        r#"{{"status":"reserved","feature":"{}","message":"{} is reserved for the Sylas secondary token runtime and is not exposed in the wallet yet"}}"#,
        feature, feature
    )
}

async fn account_json(state: &AppState, key_hex: &str) -> EngineResult<String> {
    let key = decode_hex::<32>(key_hex, "account")?;
    let engine = state.engine.lock().await;
    match engine.account(&key)? {
        Some(entry) => Ok(account_entry_json(&entry, true)),
        None => Ok(format!(
            r#"{{"exists":false,"key":"{}","balance":"0","nonce":0,"version":0,"data_len":0,"data":""}}"#,
            to_hex(&key)
        )),
    }
}

async fn accounts_json(state: &AppState) -> EngineResult<String> {
    let engine = state.engine.lock().await;
    let accounts = engine.accounts(ACCOUNT_LIST_LIMIT)?;
    let body = accounts
        .iter()
        .map(|entry| account_entry_json(entry, true))
        .collect::<Vec<String>>()
        .join(",");
    Ok(format!(r#"{{"limit":{},"accounts":[{}]}}"#, ACCOUNT_LIST_LIMIT, body))
}

async fn blocks_json(state: &AppState) -> String {
    let engine = state.engine.lock().await;
    let blocks = engine.recent_blocks(50);
    let body = blocks
        .iter()
        .map(block_json)
        .collect::<Vec<String>>()
        .join(",");
    format!(r#"{{"blocks":[{}]}}"#, body)
}

async fn transactions_json(state: &AppState) -> String {
    let engine = state.engine.lock().await;
    let transactions = engine.recent_transactions(TRANSACTION_LIST_LIMIT);
    let body = transactions
        .iter()
        .map(transaction_record_json)
        .collect::<Vec<String>>()
        .join(",");
    format!(
        r#"{{"limit":{},"transactions":[{}]}}"#,
        TRANSACTION_LIST_LIMIT, body
    )
}

async fn transaction_json(state: &AppState, tx_id_hex: &str) -> EngineResult<String> {
    let tx_id = decode_hex::<32>(tx_id_hex, "transaction")?;
    let engine = state.engine.lock().await;
    match engine.transaction(&tx_id) {
        Some(record) => Ok(transaction_record_json(&record)),
        None => Err(EngineError::InvalidTransaction("transaction not found")),
    }
}

async fn account_transactions_json(state: &AppState, key_hex: &str) -> EngineResult<String> {
    let key = decode_hex::<32>(key_hex, "account")?;
    let engine = state.engine.lock().await;
    let transactions = engine.account_transactions(&key, TRANSACTION_LIST_LIMIT);
    let body = transactions
        .iter()
        .map(transaction_record_json)
        .collect::<Vec<String>>()
        .join(",");
    Ok(format!(
        r#"{{"account":"{}","limit":{},"transactions":[{}]}}"#,
        to_hex(&key),
        TRANSACTION_LIST_LIMIT,
        body
    ))
}

async fn submit_transaction(state: &AppState, body: &str) -> EngineResult<String> {
    let line = body.trim();
    if line.is_empty() {
        return Err(EngineError::InvalidTransaction("empty transaction body"));
    }
    let tx = parse_transaction_line(line)?;
    tx.verify()?;
    let tx_id = to_hex(&tx.id());
    state
        .tx_sender
        .send(tx)
        .await
        .map_err(|_| EngineError::Network("execution pipeline is closed".into()))?;
    Ok(format!(r#"{{"queued":true,"tx_id":"{}"}}"#, tx_id))
}

fn transaction_record_json(record: &TransactionRecord) -> String {
    let read_set = pubkey_array_json(&record.read_set);
    let write_set = pubkey_array_json(&record.write_set);
    let transfers = record
        .transfers
        .iter()
        .map(|transfer| {
            format!(
                r#"{{"from":"{}","to":"{}","amount":"{}"}}"#,
                to_hex(&transfer.from),
                to_hex(&transfer.to),
                transfer.amount
            )
        })
        .collect::<Vec<String>>()
        .join(",");
    let total_amount: u128 = record
        .transfers
        .iter()
        .map(|transfer| transfer.amount as u128)
        .sum();
    format!(
        r#"{{"tx_id":"{}","kind":"{}","status":"{}","success":{},"message":"{}","block_height":{},"block_hash":"{}","timestamp_ms":{},"stage":{},"signer":"{}","fee":"{}","amount":"{}","recent_block_hash":"{}","read_set":{},"write_set":{},"transfers":[{}]}}"#,
        to_hex(&record.tx_id),
        record.kind,
        if record.success { "success" } else { "failed" },
        record.success,
        json_escape(&record.message),
        record.block_height,
        to_hex(&record.block_hash),
        record.timestamp_ms,
        record.stage,
        to_hex(&record.signer),
        record.fee,
        total_amount,
        to_hex(&record.recent_block_hash),
        read_set,
        write_set,
        transfers
    )
}

fn pubkey_array_json(keys: &[crate::types::Pubkey]) -> String {
    let body = keys
        .iter()
        .map(|key| format!(r#""{}""#, to_hex(key)))
        .collect::<Vec<String>>()
        .join(",");
    format!("[{}]", body)
}

fn account_entry_json(entry: &StateEntry, exists: bool) -> String {
    format!(
        r#"{{"exists":{},"key":"{}","balance":"{}","nonce":{},"version":{},"data_len":{},"data":"{}"}}"#,
        exists,
        to_hex(&entry.key),
        entry.balance,
        entry.nonce,
        entry.version,
        entry.data_len,
        to_hex(&entry.data[..entry.data_len as usize])
    )
}

fn block_json(block: &Block) -> String {
    format!(
        r#"{{"height":{},"hash":"{}","previous_hash":"{}","state_root":"{}","tx_root":"{}","tx_count":{},"timestamp_ms":{}}}"#,
        block.height,
        to_hex(&block.hash),
        to_hex(&block.previous_hash),
        to_hex(&block.state_root),
        to_hex(&block.tx_root),
        block.tx_count,
        block.timestamp_ms
    )
}

struct DebugCheck {
    name: String,
    status: &'static str,
    detail: String,
}

impl DebugCheck {
    fn json(&self) -> String {
        format!(
            r#"{{"name":"{}","status":"{}","detail":"{}"}}"#,
            json_escape(&self.name),
            self.status,
            json_escape(&self.detail)
        )
    }
}

fn debug_check(name: &str, status: &'static str, detail: &str) -> DebugCheck {
    DebugCheck {
        name: name.to_string(),
        status,
        detail: detail.to_string(),
    }
}

pub async fn run_block_pipeline(
    engine: Arc<Mutex<Engine>>,
    consensus: Arc<Mutex<ConsensusEngine>>,
    mut receiver: tokio::sync::mpsc::Receiver<Transaction>,
    max_batch_size: usize,
    batch_wait: Duration,
) {
    loop {
        let Some(first) = receiver.recv().await else {
            return;
        };
        let mut batch = vec![first];
        let deadline = tokio::time::sleep(batch_wait);
        tokio::pin!(deadline);
        while batch.len() < max_batch_size {
            tokio::select! {
                maybe_tx = receiver.recv() => {
                    match maybe_tx {
                        Some(tx) => batch.push(tx),
                        None => break,
                    }
                }
                _ = &mut deadline => break,
            }
        }
        let execution_result = {
            let mut engine = engine.lock().await;
            engine.execute_block(batch)
        };
        match execution_result {
            Ok(execution) => {
                consensus.lock().await.record_commit(&execution.block);
                println!(
                    "block={} committed={} verified={} rejected={} stages={} conflicts={}",
                    execution.block.height,
                    execution.block.tx_count,
                    execution.verified_transactions,
                    execution.rejected_transactions,
                    execution.stages,
                    execution.conflict_edges
                );
            }
            Err(err) if err.to_string().contains("no verified transactions") => {
                println!("batch rejected: no verified transactions");
            }
            Err(err) => eprintln!("block execution failed: {err}"),
        }
    }
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> EngineResult<()> {
    let status_text = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    Ok(())
}

fn optional_f64_json(value: Option<f64>) -> String {
    value.map_or_else(|| "null".to_string(), |number| format!("{number:.8}"))
}

fn market_cap_json(total_supply: u128, decimals: u8, price_usd: Option<f64>) -> String {
    let Some(price_usd) = price_usd else {
        return "null".to_string();
    };
    let native_supply = (total_supply as f64) / 10f64.powi(decimals as i32);
    format!("{:.2}", native_supply * price_usd)
}

fn error_json(message: &str) -> String {
    format!(r#"{{"error":"{}"}}"#, json_escape(message))
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

const EXPLORER_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sylas Explorer</title>
  <style>
    :root{color-scheme:light;--bg:#f3f6f8;--surface:#fff;--ink:#101820;--muted:#64727d;--line:#d8e1e6;--accent:#087f73;--accent-soft:#e8f6f3;--blue:#245f9c;--danger:#a33b3b;--ok:#0f7c4f;--warn:#9f6b11;--shadow:0 18px 55px rgba(14,29,39,.08)}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;letter-spacing:0}a{color:inherit}
    header{position:sticky;top:0;z-index:5;background:#fff;border-bottom:1px solid var(--line);padding:16px clamp(16px,4vw,54px);display:flex;align-items:center;justify-content:space-between;gap:18px}
    .brand{display:flex;align-items:center;gap:12px}.mark{width:38px;height:38px;border-radius:8px;background:var(--accent);color:#fff;display:grid;place-items:center;font-weight:950}.eyebrow{margin:0;color:var(--muted);font-size:.73rem;text-transform:uppercase;font-weight:850}h1,h2,h3,p{margin:0;letter-spacing:0}h1{font-size:1.34rem}h2{font-size:1rem}h3{font-size:.88rem}
    nav{display:flex;gap:8px;flex-wrap:wrap}nav a{border:1px solid var(--line);border-radius:8px;padding:8px 11px;text-decoration:none;font-size:.82rem;font-weight:850;background:#fff}nav a.active{background:var(--accent-soft);border-color:#b8dfd8;color:#07594f}
    main{padding:18px clamp(14px,3vw,36px) 42px;max-width:1480px;margin:0 auto}.hero{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:14px;align-items:end;margin-bottom:14px}.hero p{color:var(--muted);margin-top:6px;line-height:1.45}
    .search{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;background:#fff;border:1px solid var(--line);box-shadow:var(--shadow);border-radius:8px;padding:10px;margin-bottom:14px}
    input,button,pre{font:inherit}input{width:100%;border:1px solid var(--line);border-radius:8px;padding:12px 13px;outline:none;font-family:"Cascadia Mono","SFMono-Regular",Consolas,monospace;font-size:.86rem}input:focus{border-color:#73aaa2;box-shadow:0 0 0 3px rgba(8,127,115,.12)}
    button{border:1px solid var(--line);background:#fff;color:#1b2b34;border-radius:8px;padding:10px 13px;font-weight:850;cursor:pointer}button.primary{background:var(--accent);border-color:var(--accent);color:#fff}button:hover{filter:brightness(.98)}
    .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.two{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(330px,.7fr);gap:14px;margin-top:14px}.three{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-top:14px}
    .panel{background:var(--surface);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow);padding:16px;min-width:0}.metric span,.label{display:block;color:var(--muted);font-size:.72rem;font-weight:850;text-transform:uppercase}.metric strong{display:block;margin-top:7px;font-size:1.38rem}.sub{color:var(--muted);font-size:.88rem;line-height:1.45;margin-top:7px}
    .row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid var(--line)}.row:last-child{border-bottom:0}.actions{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-top:10px}
    table{width:100%;border-collapse:collapse;font-size:.86rem}th,td{text-align:left;border-bottom:1px solid var(--line);padding:10px;vertical-align:top}th{color:var(--muted);font-size:.72rem;text-transform:uppercase}td code,pre{font-family:"Cascadia Mono","SFMono-Regular",Consolas,monospace}td code{font-size:.76rem;overflow-wrap:anywhere}.hash{overflow-wrap:anywhere}
    pre{margin:0;background:#101816;color:#dceee8;border-radius:8px;padding:14px;overflow:auto;white-space:pre-wrap;min-height:240px;font-size:.8rem}.badge{display:inline-flex;border-radius:8px;padding:5px 8px;font-size:.72rem;font-weight:900;text-transform:uppercase}.badge.pass{background:#e7f5ee;color:var(--ok);border:1px solid #b8dfcd}.badge.fail{background:#fae8e8;color:var(--danger);border:1px solid #e6b4b4}.badge.warn{background:#fff5df;color:var(--warn);border:1px solid #efd39d}
    tr.pass td{background:#fbfefc}tr.fail td{background:#fff7f7}.mono{font-family:"Cascadia Mono","SFMono-Regular",Consolas,monospace}.right{text-align:right}.nowrap{white-space:nowrap}.muted{color:var(--muted)}
    @media(max-width:1050px){.grid,.two,.three,.hero{grid-template-columns:1fr}header{align-items:flex-start;flex-direction:column}.search{grid-template-columns:1fr}table{font-size:.8rem}}
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="mark">S</div>
      <div>
        <p class="eyebrow">Sylas Mainnet</p>
        <h1>Sylas Explorer</h1>
      </div>
    </div>
    <nav>
      <a href="/">Console</a>
      <a class="active" href="/explorer">Explorer</a>
      <a href="/api/status">API Status</a>
    </nav>
  </header>
  <main>
    <section class="hero">
      <div>
        <p class="eyebrow">Search the chain</p>
        <h1>Blocks, transactions, accounts, validators, and supply</h1>
        <p>Search by transaction hash or account public key. New receipts appear here as this node processes them.</p>
      </div>
      <button id="refreshAll">Refresh</button>
    </section>

    <form class="search" id="searchForm">
      <input id="searchInput" autocomplete="off" spellcheck="false" placeholder="Transaction hash or account public key">
      <button class="primary" type="submit">Search</button>
    </form>

    <section class="grid">
      <div class="panel metric"><span>Block Height</span><strong id="height">0</strong></div>
      <div class="panel metric"><span>Indexed Transactions</span><strong id="txCount">0</strong></div>
      <div class="panel metric"><span>Accounts</span><strong id="accounts">0</strong></div>
      <div class="panel metric"><span>Total Supply</span><strong id="supply">0</strong></div>
      <div class="panel metric"><span>Recommended Fee</span><strong id="fee">0</strong></div>
      <div class="panel metric"><span>Validators</span><strong id="validators">0</strong></div>
      <div class="panel metric"><span>Peers</span><strong id="peers">0</strong></div>
      <div class="panel metric"><span>Market Cap</span><strong id="marketCap">-</strong></div>
    </section>

    <section class="two">
      <div class="panel">
        <div class="row">
          <div>
            <h2>Recent Transactions</h2>
            <p class="sub">Latest indexed receipts on this node.</p>
          </div>
          <button id="refreshTransactions">Refresh</button>
        </div>
        <table>
          <thead><tr><th>Signature</th><th>Status</th><th>Block</th><th>Type</th><th>From</th><th>To</th><th class="right">Amount</th><th class="right">Fee</th></tr></thead>
          <tbody id="transactions"><tr><td colspan="8">Loading.</td></tr></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Lookup Detail</h2>
        <p class="sub" id="lookupSummary">No search yet.</p>
        <pre id="lookupJson">{}</pre>
      </div>
    </section>

    <section class="two">
      <div class="panel">
        <h2>Recent Blocks</h2>
        <table>
          <thead><tr><th>Height</th><th>Txs</th><th>Hash</th><th>State Root</th><th>Time</th></tr></thead>
          <tbody id="blocks"><tr><td colspan="5">Loading.</td></tr></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Network</h2>
        <div class="row"><span class="label">Chain ID</span><code id="chainId">-</code></div>
        <div class="row"><span class="label">Node ID</span><code id="nodeId">-</code></div>
        <div class="row"><span class="label">Recent Block Hash</span><code class="hash" id="recentHash">-</code></div>
        <div class="row"><span class="label">State Root</span><code class="hash" id="stateRoot">-</code></div>
        <div class="row"><span class="label">Consensus Mode</span><strong id="consensusMode">-</strong></div>
      </div>
    </section>

    <section class="three">
      <div class="panel">
        <h2>Top Accounts</h2>
        <table>
          <thead><tr><th>Account</th><th class="right">Balance</th><th>Nonce</th></tr></thead>
          <tbody id="topAccounts"><tr><td colspan="3">Loading.</td></tr></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Validators</h2>
        <table>
          <thead><tr><th>Node</th><th>Power</th></tr></thead>
          <tbody id="validatorRows"><tr><td colspan="2">Loading.</td></tr></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Peers</h2>
        <table>
          <thead><tr><th>Node</th><th>Address</th><th>Height</th></tr></thead>
          <tbody id="peerRows"><tr><td colspan="3">Loading.</td></tr></tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const $=id=>document.getElementById(id);
    let statusCache=null;
    const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const clean=v=>String(v||'').trim().replace(/^sylas:/,'').toLowerCase();
    const short=v=>v&&v.length>18?v.slice(0,10)+'...'+v.slice(-8):v;
    const fmt=n=>Number(n||0).toLocaleString();
    async function json(path){const r=await fetch(path,{cache:'no-store'});const body=await r.text();if(!r.ok)throw new Error(body);return body?JSON.parse(body):{}}
    function nativeUnits(base,decimals){const n=BigInt(base||'0'),d=10n**BigInt(decimals||8),w=n/d,f=(n%d).toString().padStart(decimals||8,'0').replace(/0+$/,'');return f?w+'.'+f:String(w)}
    function time(ms){const n=Number(ms||0);return n?new Date(n).toLocaleString():'-'}
    function firstTransfer(tx){return (tx.transfers&&tx.transfers[0])||{from:'',to:'',amount:'0'}}
    function badge(tx){const cls=tx.success?'pass':'fail';return `<span class="badge ${cls}">${esc(tx.status||cls)}</span>`}
    function linkButton(label,value,kind){return `<button data-kind="${kind}" data-value="${esc(value)}">${esc(label)}</button>`}

    function renderTransactions(items){
      const decimals=statusCache?.economics?.token_decimals||8;
      $('txCount').textContent=fmt(items?.length||0);
      if(!items||!items.length){$('transactions').innerHTML='<tr><td colspan="8">No transactions indexed yet.</td></tr>';return}
      $('transactions').innerHTML=items.map(tx=>{
        const t=firstTransfer(tx),from=tx.kind==='airdrop'?'mint':short(t.from),to=short(t.to);
        return `<tr class="${tx.success?'pass':'fail'}"><td>${linkButton(short(tx.tx_id),tx.tx_id,'tx')}</td><td>${badge(tx)}</td><td>${tx.block_height}</td><td>${esc(tx.kind)}</td><td><code>${esc(from)}</code></td><td><code>${esc(to)}</code></td><td class="right">${nativeUnits(tx.amount,decimals)}</td><td class="right">${nativeUnits(tx.fee,decimals)}</td></tr>`
      }).join('');
    }

    function renderBlocks(items){
      if(!items||!items.length){$('blocks').innerHTML='<tr><td colspan="5">No committed blocks yet.</td></tr>';return}
      $('blocks').innerHTML=items.map(block=>`<tr><td>${block.height}</td><td>${block.tx_count}</td><td><code>${esc(short(block.hash))}</code></td><td><code>${esc(short(block.state_root))}</code></td><td class="nowrap">${time(block.timestamp_ms)}</td></tr>`).join('');
    }

    function renderAccounts(items){
      const decimals=statusCache?.economics?.token_decimals||8;
      if(!items||!items.length){$('topAccounts').innerHTML='<tr><td colspan="3">No funded accounts.</td></tr>';return}
      $('topAccounts').innerHTML=items.map(account=>`<tr><td>${linkButton(short(account.key),account.key,'account')}</td><td class="right">${nativeUnits(account.balance,decimals)}</td><td>${account.nonce}</td></tr>`).join('');
    }

    function renderValidators(items){
      if(!items||!items.length){$('validatorRows').innerHTML='<tr><td colspan="2">No validators configured.</td></tr>';return}
      $('validatorRows').innerHTML=items.map(v=>`<tr><td><code>${esc(v.node_id||v.id||'-')}</code></td><td>${esc(v.voting_power??v.power??'-')}</td></tr>`).join('');
    }

    function renderPeers(items){
      if(!items||!items.length){$('peerRows').innerHTML='<tr><td colspan="3">No peers connected.</td></tr>';return}
      $('peerRows').innerHTML=items.map(p=>`<tr><td><code>${esc(p.node_id||'-')}</code></td><td><code>${esc(p.address||'-')}</code></td><td>${esc(p.height??'-')}</td></tr>`).join('');
    }

    async function openTransaction(hash){
      const tx=await json('/api/transaction/'+clean(hash));
      $('lookupSummary').textContent=`Transaction ${short(tx.tx_id)} in block ${tx.block_height}`;
      $('lookupJson').textContent=JSON.stringify(tx,null,2);
      renderTransactions([tx]);
      history.replaceState(null,'','/explorer?q='+encodeURIComponent(tx.tx_id));
    }

    async function openAccount(key){
      const account=await json('/api/account/'+clean(key));
      let historyRows={transactions:[]};
      try{historyRows=await json('/api/account/'+clean(key)+'/transactions')}catch(e){}
      $('lookupSummary').textContent=`Account ${short(account.key)} with ${historyRows.transactions.length} indexed transactions`;
      $('lookupJson').textContent=JSON.stringify({account,history:historyRows},null,2);
      renderTransactions(historyRows.transactions);
      history.replaceState(null,'','/explorer?q='+encodeURIComponent(account.key));
    }

    async function search(value){
      const query=clean(value);
      if(!query){$('lookupSummary').textContent='Enter a transaction hash or account public key.';return}
      if(!/^[0-9a-f]{64}$/.test(query)){$('lookupSummary').textContent='Search must be a 32-byte hex transaction hash or account public key.';return}
      try{await openTransaction(query);return}catch(txErr){}
      try{await openAccount(query);return}catch(accountErr){$('lookupSummary').textContent='No transaction or account found.';$('lookupJson').textContent=String(accountErr)}
    }

    async function load(){
      const [status,fees,blocks,accounts,transactions,validators,peers]=await Promise.all([
        json('/api/status'),json('/api/fees'),json('/api/blocks'),json('/api/accounts'),json('/api/transactions'),json('/api/validators'),json('/api/peers')
      ]);
      statusCache=status;
      const decimals=status.economics.token_decimals;
      $('height').textContent=fmt(status.height);
      $('accounts').textContent=fmt(status.accounts);
      $('supply').textContent=nativeUnits(status.economics.total_supply_base_units,decimals);
      $('marketCap').textContent=status.economics.market_cap_usd===null?'-':'$'+fmt(status.economics.market_cap_usd);
      $('fee').textContent=nativeUnits(fees.recommended_fee,decimals);
      $('validators').textContent=fmt(status.consensus.validator_count);
      $('peers').textContent=fmt(status.network.peers);
      $('chainId').textContent=status.chain_id;
      $('nodeId').textContent=status.network.node_id;
      $('recentHash').textContent=status.recent_block_hash;
      $('stateRoot').textContent=status.state_root;
      $('consensusMode').textContent=status.consensus.mode;
      renderTransactions(transactions.transactions);
      renderBlocks(blocks.blocks);
      renderAccounts(accounts.accounts);
      renderValidators(validators.validators);
      renderPeers(peers.peers);
    }

    document.addEventListener('click',e=>{
      const button=e.target.closest('button[data-kind]');
      if(!button)return;
      if(button.dataset.kind==='tx')openTransaction(button.dataset.value).catch(err=>{$('lookupSummary').textContent=String(err)});
      if(button.dataset.kind==='account')openAccount(button.dataset.value).catch(err=>{$('lookupSummary').textContent=String(err)});
    });
    $('searchForm').addEventListener('submit',e=>{e.preventDefault();search($('searchInput').value)});
    $('refreshAll').onclick=load;
    $('refreshTransactions').onclick=async()=>renderTransactions((await json('/api/transactions')).transactions);
    const initial=new URLSearchParams(location.search).get('q');
    if(initial)$('searchInput').value=initial;
    load().then(()=>{if(initial)search(initial)}).catch(err=>{$('lookupSummary').textContent=String(err);$('lookupJson').textContent=String(err)});
    setInterval(()=>load().catch(()=>{}),5000);
  </script>
</body>
</html>"##;

const INDEX_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sylas Network Console</title>
  <style>
    :root{color-scheme:light;--bg:#f4f7f8;--surface:#fff;--soft:#eef4f3;--ink:#11191f;--muted:#64727d;--line:#d7e0e5;--accent:#087f73;--accent-2:#265f9d;--danger:#a33b3b;--ok:#0f7c4f;--warn:#9f6b11;--shadow:0 18px 55px rgba(14,29,39,.08)}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;letter-spacing:0}
    header{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:18px clamp(16px,4vw,52px);border-bottom:1px solid var(--line);background:#fff;position:sticky;top:0;z-index:5}
    h1,h2,h3,p{margin:0;letter-spacing:0}h1{font-size:1.45rem}h2{font-size:1rem}h3{font-size:.9rem}.eyebrow{color:var(--muted);font-size:.75rem;font-weight:850;text-transform:uppercase;margin-bottom:4px}
    .pill{display:inline-flex;align-items:center;gap:7px;border:1px solid #b8dfd8;background:#e8f6f3;color:#07594f;border-radius:8px;padding:8px 11px;font-weight:850;font-size:.82rem}
    .layout{display:grid;grid-template-columns:250px minmax(0,1fr);min-height:calc(100vh - 74px)}nav{border-right:1px solid var(--line);padding:18px;background:#fbfcfd}
    nav a{display:block;color:#2d3d47;text-decoration:none;border-radius:8px;padding:10px 12px;font-weight:750;margin-bottom:5px}nav a.active,nav a:hover{background:#eaf2f1;color:#07594f}
    main{padding:18px clamp(14px,3vw,36px) 42px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.two{display:grid;grid-template-columns:minmax(0,1.18fr) minmax(330px,.82fr);gap:14px;margin-top:14px}.three{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-top:14px}
    .panel{background:var(--surface);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow);padding:16px;min-width:0}.metric span,.label{display:block;color:var(--muted);font-size:.75rem;font-weight:850;text-transform:uppercase}.metric strong{display:block;margin-top:7px;font-size:1.42rem}.sub{color:var(--muted);font-size:.9rem;line-height:1.45;margin-top:8px}
    .row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid var(--line)}.row:last-child{border-bottom:0}.status-line{display:grid;gap:4px}.section{margin-top:22px}.wide{grid-column:1/-1}
    code,pre,textarea,input{font-family:"Cascadia Mono","SFMono-Regular",Consolas,monospace}code,.hash{overflow-wrap:anywhere}pre{margin:0;background:#0f1715;color:#dceee8;border-radius:8px;padding:14px;overflow:auto;min-height:190px;white-space:pre-wrap;overflow-wrap:anywhere}
    table{width:100%;border-collapse:collapse;font-size:.88rem}th,td{text-align:left;border-bottom:1px solid var(--line);padding:10px;vertical-align:top}th{color:var(--muted);font-size:.72rem;text-transform:uppercase}td code{font-size:.78rem}
    input,textarea{width:100%;border:1px solid var(--line);border-radius:8px;background:#fff;color:var(--ink);padding:10px;font-size:.86rem;outline:none}textarea{min-height:84px;resize:vertical}input:focus,textarea:focus{border-color:#73aaa2;box-shadow:0 0 0 3px rgba(8,127,115,.12)}
    .form{display:grid;gap:10px;margin-top:12px}.split{display:grid;grid-template-columns:1fr 1fr;gap:10px}.actions{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-top:12px}
    button{border:1px solid var(--line);background:#fff;color:#1b2b34;border-radius:8px;padding:9px 12px;font-weight:850;cursor:pointer}button.primary{background:var(--accent);border-color:var(--accent);color:#fff}button:hover{filter:brightness(.98)}.ok{color:var(--ok);font-weight:850}.warn{color:var(--warn);font-weight:850}.danger{color:var(--danger);font-weight:850}
    .badge{display:inline-flex;align-items:center;justify-content:center;border-radius:8px;padding:5px 8px;font-size:.72rem;font-weight:900;text-transform:uppercase}.badge.pass{background:#e7f5ee;color:var(--ok);border:1px solid #b8dfcd}.badge.warn{background:#fff5df;color:var(--warn);border:1px solid #efd39d}.badge.fail{background:#fae8e8;color:var(--danger);border:1px solid #e6b4b4}
    tr.fail td{background:#fff7f7}tr.warn td{background:#fffaf0}tr.pass td{background:#fbfefc}
    @media(max-width:1080px){.layout{grid-template-columns:1fr}nav{display:none}.grid,.two,.three,.split{grid-template-columns:1fr}header{align-items:flex-start;flex-direction:column}}
  </style>
</head>
<body>
  <header>
    <div>
      <p class="eyebrow">Sylas network console</p>
      <h1>Chain, Wallet, Explorer, Markets</h1>
    </div>
    <div class="pill" id="nodePill">Connecting</div>
  </header>
  <div class="layout">
    <nav>
      <a class="active" href="#overview">Overview</a>
      <a href="#wallet">Wallet</a>
      <a href="#markets">Markets</a>
      <a href="#network">Network</a>
      <a href="#debug">Debug</a>
      <a href="#explorer">Explorer</a>
      <a href="#execution">Execution</a>
      <a href="#interfaces">Interfaces</a>
      <a href="#security">Security</a>
    </nav>
    <main>
      <section id="overview" class="grid">
        <div class="panel metric"><span>Height</span><strong id="height">0</strong></div>
        <div class="panel metric"><span>Accounts</span><strong id="accounts">0</strong></div>
        <div class="panel metric"><span>Total Supply</span><strong id="supply">0</strong></div>
        <div class="panel metric"><span>Market Cap</span><strong id="marketCap">-</strong></div>
      </section>

      <section class="two">
        <div class="panel">
          <h2>Ledger Commitments</h2>
          <div class="status-line">
            <div class="row"><span class="label">Chain ID</span><code id="chainId">-</code></div>
            <div class="row"><span class="label">Recent Block Hash</span><code class="hash" id="recentHash">-</code></div>
            <div class="row"><span class="label">State Root</span><code class="hash" id="stateRoot">-</code></div>
          </div>
        </div>
        <div class="panel">
          <h2>Node Health</h2>
          <pre id="statusJson">{}</pre>
        </div>
      </section>

      <section id="wallet" class="two">
        <div class="panel">
          <h2>Sylas Wallet Extension</h2>
          <div class="status-line">
            <div class="row"><span class="label">Status</span><strong id="walletConnection">Not connected</strong></div>
            <div class="row"><span class="label">Address</span><code id="walletAddress">-</code></div>
            <div class="row"><span class="label">Balance</span><strong id="walletBalance">-</strong></div>
          </div>
          <div class="actions">
            <button class="primary" id="connectWallet">Connect Wallet</button>
            <button id="refreshWalletButton">Refresh Wallet</button>
          </div>
          <p class="sub" id="walletStatus">Use the Sylas Wallet extension. The dashboard never receives private keys.</p>
        </div>
        <div class="panel">
          <h2>Send Transfer</h2>
          <div class="form">
            <label><span class="label">Recipient Public Key</span><input id="recipient" autocomplete="off" spellcheck="false"></label>
            <label><span class="label">Amount</span><input id="amount" value="0" inputmode="decimal"></label>
          </div>
          <div class="actions"><button class="primary" id="sendTransfer">Request Wallet Signature</button></div>
          <p class="sub" id="transferStatus">Transfers are signed inside the extension provider.</p>
        </div>
      </section>

      <section id="markets" class="three">
        <div class="panel"><h2>Supply</h2><p class="sub"><strong id="supplyDetail">0</strong></p></div>
        <div class="panel"><h2>Price Source</h2><p class="sub"><strong id="priceDetail">Not configured</strong></p></div>
        <div class="panel"><h2>Valuation</h2><p class="sub"><strong id="valuationDetail">Unavailable</strong></p></div>
      </section>

      <section id="network" class="two">
        <div class="panel">
          <h2>Validator Network</h2>
          <div class="status-line">
            <div class="row"><span class="label">Node ID</span><code id="nodeId">-</code></div>
            <div class="row"><span class="label">Consensus Mode</span><strong id="consensusMode">-</strong></div>
            <div class="row"><span class="label">Validators</span><strong id="validatorCount">0</strong></div>
            <div class="row"><span class="label">Peers</span><strong id="peerCount">0</strong></div>
          </div>
        </div>
        <div class="panel">
          <h2>Peer Book</h2>
          <table>
            <thead><tr><th>Node</th><th>Address</th><th>Height</th></tr></thead>
            <tbody id="peers"><tr><td colspan="3">No peers connected.</td></tr></tbody>
          </table>
        </div>
      </section>

      <section id="debug" class="section panel">
        <div class="row">
          <div>
            <h2>Diagnostics</h2>
            <p class="sub" id="debugSummary">Not run.</p>
          </div>
          <button class="primary" id="runDebug">Run Diagnostics</button>
        </div>
        <table>
          <thead><tr><th>Status</th><th>Check</th><th>Detail</th></tr></thead>
          <tbody id="debugRows"><tr><td colspan="3">No diagnostics yet.</td></tr></tbody>
        </table>
        <pre id="debugJson">{}</pre>
      </section>

      <section id="explorer" class="two">
        <div class="panel">
          <h2>Account Lookup</h2>
          <div class="form">
            <label><span class="label">Account Public Key</span><input id="lookupKey" autocomplete="off" spellcheck="false"></label>
          </div>
          <div class="actions"><button id="lookupAccount">Lookup Account</button><button id="lookupAccountTxs">Account Transactions</button></div>
          <pre id="accountJson">{}</pre>
        </div>
        <div class="panel">
          <h2>Transaction Lookup</h2>
          <div class="form">
            <label><span class="label">Transaction Hash</span><input id="txSearch" autocomplete="off" spellcheck="false"></label>
          </div>
          <div class="actions"><button class="primary" id="lookupTx">Lookup Transaction</button><button id="refreshTxs">Refresh Recent</button></div>
          <pre id="transactionJson">{}</pre>
        </div>
      </section>

      <section class="section panel">
        <div class="row">
          <div>
            <h2>Recent Transactions</h2>
            <p class="sub">Confirmed transfers and airdrops retained by this node.</p>
          </div>
          <button id="refreshTxsTop">Refresh</button>
        </div>
        <table>
          <thead><tr><th>Tx</th><th>Status</th><th>Block</th><th>From</th><th>To</th><th>Amount</th><th>Fee</th></tr></thead>
          <tbody id="recentTransactions"><tr><td colspan="7">No transactions indexed yet.</td></tr></tbody>
        </table>
      </section>

      <section class="section panel">
        <h2>Top Accounts</h2>
        <table>
          <thead><tr><th>Account</th><th>Balance</th><th>Nonce</th></tr></thead>
          <tbody id="topAccounts"><tr><td colspan="3">Loading.</td></tr></tbody>
        </table>
      </section>

      <section class="section panel">
        <h2>Recent Blocks</h2>
        <table>
          <thead><tr><th>Height</th><th>Txs</th><th>Hash</th><th>State Root</th></tr></thead>
          <tbody id="blocks"><tr><td colspan="4">Awaiting committed blocks.</td></tr></tbody>
        </table>
      </section>

      <section id="execution" class="three">
        <div class="panel"><h2>Parallel Runtime</h2><p class="sub">Explicit account access sets; disjoint writes execute across Rayon workers.</p></div>
        <div class="panel"><h2>Conflict Control</h2><p class="sub">Write conflicts are converted into deterministic stages.</p></div>
        <div class="panel"><h2>Replay Protection</h2><p class="sub">Signatures bind chain ID, nonce, recent hash, access sets, and transfers.</p></div>
      </section>

      <section id="interfaces" class="three">
        <div class="panel"><h2>HTTP API</h2><p class="sub"><code>GET /api/status</code><br><code>GET /api/debug</code><br><code>GET /api/consensus</code><br><code>GET /api/validators</code><br><code>GET /api/peers</code><br><code>GET /api/economics</code><br><code>GET /api/fees</code><br><code>GET /api/accounts</code><br><code>GET /api/transactions</code><br><code>GET /api/transaction/&lt;hash&gt;</code><br><code>GET /api/account/&lt;pubkey&gt;/transactions</code><br><code>POST /api/airdrop</code><br><code>POST /api/transactions</code></p></div>
        <div class="panel"><h2>TCP Ingress</h2><p class="sub"><code id="txBind">-</code><br>Line-delimited signed transactions.</p></div>
        <div class="panel">
          <h2>Raw Transaction</h2>
          <textarea id="rawTx" spellcheck="false"></textarea>
          <div class="actions"><button id="queueRawTx">Queue Raw TX</button></div>
          <p class="sub" id="rawStatus">Ready.</p>
        </div>
      </section>

      <section id="security" class="section panel">
        <h2>Security Posture</h2>
        <table>
          <tbody>
            <tr><th>Signatures</th><td>Ed25519 verification before scheduling</td></tr>
            <tr><th>Hashing</th><td>Domain-separated SHA-256 commitments</td></tr>
            <tr><th>State</th><td>Memory-mapped state roots after every committed block</td></tr>
            <tr><th>Scheduler</th><td>Declared write-set conflict isolation</td></tr>
          </tbody>
        </table>
      </section>
    </main>
  </div>
  <script>
    const CHAIN_ID='sylas-mainnet';
    const $=id=>document.getElementById(id);
    let wallet=null;
    let statusCache=null;

    const fmt=n=>Number(n||0).toLocaleString();
    const short=h=>h&&h.length>18?h.slice(0,10)+'...'+h.slice(-8):h;
    const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const cleanHex=v=>(v||'').trim().toLowerCase();
    async function json(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error(await r.text());return r.json()}
    const fromHex=input=>{const h=cleanHex(input);if(h.length%2||!/^[0-9a-f]*$/.test(h))throw new Error('bad hex');const out=new Uint8Array(h.length/2);for(let i=0;i<out.length;i++)out[i]=parseInt(h.slice(i*2,i*2+2),16);return out};
    function baseUnits(value,decimals){const raw=String(value||'0').trim();if(!/^\d+(\.\d+)?$/.test(raw))throw new Error('bad amount');const [whole,frac='']=raw.split('.');if(frac.length>decimals)throw new Error('too many decimal places');return BigInt(whole||'0')*(10n**BigInt(decimals))+BigInt((frac+'0'.repeat(decimals)).slice(0,decimals)||'0')}
    function nativeUnits(base,decimals){const n=BigInt(base||'0'),d=10n**BigInt(decimals),w=n/d,f=(n%d).toString().padStart(decimals,'0').replace(/0+$/,'');return f?w+'.'+f:String(w)}
    function renderWallet(){if(wallet){$('walletConnection').textContent='Connected';$('walletAddress').textContent=short(wallet.publicKey);refreshWalletBalance()}else{$('walletConnection').textContent='Not connected';$('walletAddress').textContent='-';$('walletBalance').textContent='-'}}
    async function connectWallet(){if(!window.sylas)throw new Error('Sylas Wallet extension provider not found');wallet=await window.sylas.connect();$('walletStatus').textContent='Connected through Sylas Wallet extension.';renderWallet()}
    async function refreshWalletBalance(){if(!wallet||!statusCache)return;try{const a=window.sylas?await window.sylas.getAccount():await json('/api/account/'+wallet.publicKey);$('walletBalance').textContent=nativeUnits(a.balance,statusCache.economics.token_decimals)+' '+statusCache.economics.token_symbol;$('walletStatus').textContent='Balance refreshed for '+short(wallet.publicKey)}catch(e){$('walletStatus').textContent=String(e)}}
    async function sendTransfer(){
      try{
        if(!wallet)await connectWallet();
        if(!window.sylas)throw new Error('Sylas Wallet extension provider not found');
        const recipient=cleanHex($('recipient').value);if(recipient.length!==64)throw new Error('recipient must be 32-byte hex');
        fromHex(recipient);
        const status=await json('/api/status');statusCache=status;
        const amount=baseUnits($('amount').value,status.economics.token_decimals);
        const result=await window.sylas.signAndSendTransfer({to:recipient,amountBaseUnits:amount.toString(),label:'Sylas console transfer'});
        $('txSearch').value=result.tx_id;$('transferStatus').textContent='Queued '+result.tx_id;
        setTimeout(load,700);
      }catch(e){$('transferStatus').textContent=String(e)}
    }
    async function submitTxLine(line){const r=await fetch('/api/transactions',{method:'POST',headers:{'content-type':'text/plain'},body:line});const body=await r.json();if(!r.ok)throw new Error(body.error||'transaction rejected');return body}
    async function queueRaw(){try{const result=await submitTxLine($('rawTx').value);$('rawStatus').textContent='Queued '+result.tx_id;$('txSearch').value=result.tx_id;setTimeout(load,700);setTimeout(()=>lookupTransactionHash(result.tx_id),900)}catch(e){$('rawStatus').textContent=String(e)}}
    async function lookupAccount(){try{$('accountJson').textContent=JSON.stringify(await json('/api/account/'+cleanHex($('lookupKey').value)),null,2)}catch(e){$('accountJson').textContent=String(e)}}
    function primaryTransfer(tx){return (tx.transfers&&tx.transfers[0])||{from:'',to:'',amount:'0'}}
    function renderTransactions(items){
      const decimals=statusCache?.economics?.token_decimals||8;
      if(!items||!items.length){$('recentTransactions').innerHTML='<tr><td colspan="7">No transactions indexed yet.</td></tr>';return}
      $('recentTransactions').innerHTML=items.map(tx=>{
        const t=primaryTransfer(tx),status=tx.success?'pass':'fail',from=tx.kind==='airdrop'?'mint':short(t.from),to=short(t.to);
        return `<tr class="${status}"><td><button onclick="lookupTransactionHash('${esc(tx.tx_id)}')">${esc(short(tx.tx_id))}</button></td><td><span class="badge ${status}">${esc(tx.status)}</span></td><td>${tx.block_height}</td><td><code>${esc(from)}</code></td><td><code>${esc(to)}</code></td><td>${nativeUnits(tx.amount,decimals)}</td><td>${nativeUnits(tx.fee,decimals)}</td></tr>`
      }).join('');
    }
    async function refreshTransactions(){try{const result=await json('/api/transactions');renderTransactions(result.transactions);$('transactionJson').textContent=JSON.stringify(result,null,2)}catch(e){$('transactionJson').textContent=String(e)}}
    async function lookupTransaction(){await lookupTransactionHash(cleanHex($('txSearch').value))}
    async function lookupTransactionHash(hash){try{const tx=await json('/api/transaction/'+cleanHex(hash));$('txSearch').value=tx.tx_id;$('transactionJson').textContent=JSON.stringify(tx,null,2);renderTransactions([tx])}catch(e){$('transactionJson').textContent=String(e)}}
    async function lookupAccountTransactions(){try{const result=await json('/api/account/'+cleanHex($('lookupKey').value)+'/transactions');$('accountJson').textContent=JSON.stringify(result,null,2);renderTransactions(result.transactions)}catch(e){$('accountJson').textContent=String(e)}}
    async function browserDiagnostics(){
      const checks=[];
      const webcrypto=globalThis.crypto&&globalThis.crypto.subtle;
      checks.push({name:'browser.webcrypto',status:webcrypto?'pass':'fail',detail:webcrypto?'WebCrypto available':'WebCrypto unavailable'});
      try{localStorage.setItem('sylas.debug','ok');localStorage.removeItem('sylas.debug');checks.push({name:'browser.local_storage',status:'pass',detail:'local wallet storage available'})}catch(e){checks.push({name:'browser.local_storage',status:'fail',detail:String(e)})}
      try{if(!webcrypto)throw new Error('WebCrypto unavailable');await webcrypto.generateKey({name:'Ed25519'},true,['sign','verify']);checks.push({name:'browser.ed25519',status:'pass',detail:'Ed25519 signing supported'})}catch(e){checks.push({name:'browser.ed25519',status:'warn',detail:'Ed25519 browser signing unavailable: '+String(e)})}
      checks.push({name:'browser.wallet_loaded',status:wallet?'pass':'warn',detail:wallet?'wallet present in browser':'no wallet loaded'});
      return checks;
    }
    function renderDebug(result){
      const checks=result.checks||[];
      const failures=checks.filter(x=>x.status==='fail').length,warnings=checks.filter(x=>x.status==='warn').length;
      const status=failures?'fail':warnings?'warn':'pass';
      $('debugSummary').textContent=`${status.toUpperCase()} - ${failures} failures, ${warnings} warnings, ${checks.length} checks`;
      $('debugRows').innerHTML=checks.map(x=>`<tr class="${esc(x.status)}"><td><span class="badge ${esc(x.status)}">${esc(x.status)}</span></td><td><code>${esc(x.name)}</code></td><td>${esc(x.detail)}</td></tr>`).join('');
      $('debugJson').textContent=JSON.stringify(result,null,2);
    }
    async function runDebug(){
      $('debugSummary').textContent='Running diagnostics.';
      $('debugRows').innerHTML='<tr><td colspan="3">Running.</td></tr>';
      try{
        const node=await json('/api/debug');
        const browser=await browserDiagnostics();
        const checks=[...(node.checks||[]),...browser];
        renderDebug({...node,checks});
      }catch(e){
        renderDebug({status:'fail',failures:1,warnings:0,checks:[{name:'debug.endpoint',status:'fail',detail:String(e)}]});
      }
    }

    async function load(){
      try{
        const s=await json('/api/status');statusCache=s;$('nodePill').textContent='Online';
        $('height').textContent=fmt(s.height);$('accounts').textContent=fmt(s.accounts);$('chainId').textContent=s.chain_id;$('recentHash').textContent=s.recent_block_hash;$('stateRoot').textContent=s.state_root;$('txBind').textContent=s.tx_bind;
        $('statusJson').textContent=JSON.stringify(s,null,2);
        $('supply').textContent=nativeUnits(s.economics.total_supply_base_units,s.economics.token_decimals);
        $('supplyDetail').textContent=nativeUnits(s.economics.total_supply_base_units,s.economics.token_decimals)+' '+s.economics.token_symbol;
        $('priceDetail').textContent=s.economics.price_usd===null?'Not configured':'$'+Number(s.economics.price_usd).toLocaleString();
        $('marketCap').textContent=s.economics.market_cap_usd===null?'-':'$'+Number(s.economics.market_cap_usd).toLocaleString();
        $('valuationDetail').textContent=s.economics.market_cap_usd===null?'External price required':'$'+Number(s.economics.market_cap_usd).toLocaleString();
        $('nodeId').textContent=s.network.node_id;$('peerCount').textContent=fmt(s.network.peers);$('validatorCount').textContent=fmt(s.consensus.validator_count);$('consensusMode').textContent=s.consensus.mode;
        const b=await json('/api/blocks');
        $('blocks').innerHTML=b.blocks.length?b.blocks.map(x=>`<tr><td>${x.height}</td><td>${x.tx_count}</td><td><code>${x.hash}</code></td><td><code>${x.state_root}</code></td></tr>`).join(''):'<tr><td colspan="4">Awaiting committed blocks.</td></tr>';
        const a=await json('/api/accounts');
        $('topAccounts').innerHTML=a.accounts.length?a.accounts.map(x=>`<tr><td><code>${x.key}</code></td><td>${nativeUnits(x.balance,s.economics.token_decimals)}</td><td>${x.nonce}</td></tr>`).join(''):'<tr><td colspan="3">No funded accounts.</td></tr>';
        const t=await json('/api/transactions');
        renderTransactions(t.transactions);
        const p=await json('/api/peers');
        $('peers').innerHTML=p.peers.length?p.peers.map(x=>`<tr><td><code>${x.node_id}</code></td><td><code>${x.address}</code></td><td>${x.height}</td></tr>`).join(''):'<tr><td colspan="3">No peers connected.</td></tr>';
        refreshWalletBalance();
      }catch(e){$('nodePill').textContent='Offline';$('statusJson').textContent=String(e)}
    }

    $('connectWallet').onclick=()=>connectWallet().catch(e=>{$('walletStatus').textContent=String(e)});
    $('refreshWalletButton').onclick=refreshWalletBalance;
    $('sendTransfer').onclick=sendTransfer;$('lookupAccount').onclick=lookupAccount;$('lookupAccountTxs').onclick=lookupAccountTransactions;$('lookupTx').onclick=lookupTransaction;$('refreshTxs').onclick=refreshTransactions;$('refreshTxsTop').onclick=refreshTransactions;$('queueRawTx').onclick=queueRaw;$('runDebug').onclick=runDebug;
    renderWallet();load();setInterval(load,5000);
  </script>
</body>
</html>"##;
