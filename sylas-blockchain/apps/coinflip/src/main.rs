use std::{
    collections::{BTreeMap, VecDeque},
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::{Signer, SigningKey};
use getrandom::getrandom;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

type AppResult<T> = Result<T, String>;
type Hash = [u8; 32];
type Pubkey = [u8; 32];

const CHAIN_ID: &[u8] = b"sylas-mainnet";
const TOKEN_SYMBOL: &str = "SYLAS";
const TOKEN_DECIMALS: u8 = 8;
const MIN_BET_BASE_UNITS: u64 = 10_000;
const MAX_BET_BASE_UNITS: u64 = 10_000_000;
const HOUSE_EDGE_BPS: u64 = 200;
const PAYOUT_BPS: u64 = 20_000 - HOUSE_EDGE_BPS;
const MAX_HTTP_REQUEST_BYTES: usize = 64 * 1024;
const MAX_ROUNDS: usize = 1_024;
const MAX_COMMITMENTS: usize = 1_024;

#[tokio::main]
async fn main() -> AppResult<()> {
    let bind = env::var("SYLAS_COINFLIP_BIND").unwrap_or_else(|_| "0.0.0.0:5080".to_string());
    let node_url = env::var("SYLAS_NODE_URL").unwrap_or_else(|_| "http://127.0.0.1:5000".to_string());
    let house_secret = env::var("SYLAS_COINFLIP_HOUSE_SECRET_HEX")
        .map_err(|_| "SYLAS_COINFLIP_HOUSE_SECRET_HEX is required for isolated coinflip payouts".to_string())?;
    let house_seed = decode_hex_32(&house_secret, "house_secret")?;
    let state = Arc::new(AppState {
        node: NodeClient::parse(&node_url)?,
        game: Mutex::new(CoinflipGame::new(house_seed)?),
    });

    let listener = TcpListener::bind(&bind)
        .await
        .map_err(|err| format!("bind {bind}: {err}"))?;
    println!("Sylas isolated coinflip listening on {bind}; node={node_url}");
    loop {
        let (stream, _) = listener.accept().await.map_err(|err| err.to_string())?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, state).await {
                eprintln!("coinflip http error: {err}");
            }
        });
    }
}

struct AppState {
    node: NodeClient,
    game: Mutex<CoinflipGame>,
}

struct HttpRequest {
    method: String,
    path: String,
    body: String,
}

async fn handle_connection(mut stream: TcpStream, state: Arc<AppState>) -> AppResult<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => HttpResponse::html(INDEX_HTML),
        ("GET", "/healthz") => HttpResponse::text(200, "ok\n"),
        ("GET", "/api/config") => json_response(config_json(&state).await).await,
        ("GET", "/api/rounds") => {
            let game = state.game.lock().await;
            HttpResponse::json(json!({ "rounds": game.rounds_json() }))
        }
        ("POST", "/api/commit") => {
            let commitment = state.game.lock().await.create_commitment()?;
            HttpResponse::json(json!({ "ok": true, "commitment": commitment.json() }))
        }
        ("POST", "/api/settle") => json_response(settle_round(&state, &request.body).await).await,
        ("POST", "/api/claim") => json_response(claim_round(&state, &request.body).await).await,
        _ => HttpResponse::text(404, "not found\n"),
    };
    write_response(&mut stream, response).await
}

async fn json_response(result: AppResult<Value>) -> HttpResponse {
    match result {
        Ok(value) => HttpResponse::json(value),
        Err(err) => HttpResponse::json_status(400, json!({ "error": err })),
    }
}

async fn config_json(state: &Arc<AppState>) -> AppResult<Value> {
    let house = state.game.lock().await.house_pubkey();
    let fees = state.node.get_json("/api/fees").await.unwrap_or_else(|_| json!({}));
    let status = state.node.get_json("/api/status").await.unwrap_or_else(|_| json!({}));
    let account = state
        .node
        .get_json(&format!("/api/account/{}", to_hex(&house)))
        .await
        .unwrap_or_else(|_| json!({ "balance": "0" }));
    Ok(json!({
        "network": "Sylas Mainnet",
        "house_pubkey": to_hex(&house),
        "house_balance": string_field(&account, "balance").unwrap_or_else(|| "0".to_string()),
        "min_bet": MIN_BET_BASE_UNITS.to_string(),
        "max_bet": MAX_BET_BASE_UNITS.to_string(),
        "house_edge_bps": HOUSE_EDGE_BPS,
        "payout_bps": PAYOUT_BPS,
        "token_symbol": TOKEN_SYMBOL,
        "token_decimals": TOKEN_DECIMALS,
        "recommended_fee": string_or_number(&fees, "recommended_fee").unwrap_or_else(|| "5000".to_string()),
        "node_height": status.get("height").cloned().unwrap_or_else(|| json!(0)),
        "node_status": status.get("status").cloned().unwrap_or_else(|| json!("unknown")),
    }))
}

async fn settle_round(state: &Arc<AppState>, body: &str) -> AppResult<Value> {
    let request: Value = serde_json::from_str(body).map_err(|err| format!("bad settle json: {err}"))?;
    let player_hex = required_string(&request, "player")?.to_ascii_lowercase();
    let player = decode_hex_32(&player_hex, "player")?;
    let side = CoinSide::parse(&required_string(&request, "side")?)?;
    let client_seed = required_string(&request, "client_seed")?;
    if client_seed.trim().is_empty() || client_seed.len() > 128 {
        return Err("client seed must be 1 to 128 characters".to_string());
    }
    let stake_tx_id = required_string(&request, "stake_tx_id")?;
    let commitment_id = decode_hex_32(&required_string(&request, "commitment_id")?, "commitment_id")?;
    let stake = state
        .node
        .get_json(&format!("/api/transaction/{stake_tx_id}"))
        .await?;
    let status = state.node.get_json("/api/status").await?;
    let recent_hash = decode_hex_32(
        &required_string(&status, "recent_block_hash")?,
        "recent_block_hash",
    )?;
    let house = state.game.lock().await.house_pubkey();
    let stake_transfer = extract_stake(&stake, &player_hex, &to_hex(&house))?;

    if stake_transfer.amount < MIN_BET_BASE_UNITS || stake_transfer.amount > MAX_BET_BASE_UNITS {
        let existing_refund = {
            let game = state.game.lock().await;
            game.refunds.get(&stake_transfer.tx_id).copied()
        };
        let refund_tx_id = if let Some(tx_id) = existing_refund {
            tx_id
        } else {
            let fee = state
                .node
                .get_json("/api/fees")
                .await
                .ok()
                .and_then(|value| string_or_number(&value, "recommended_fee"))
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(5_000);
            let refund_amount = stake_transfer.amount.saturating_sub(fee);
            if refund_amount == 0 {
                return Err("invalid stake is too small to refund after fees".to_string());
            }
            let signing_key = state.game.lock().await.signing_key.clone();
            let tx_id = sign_and_submit_transfer(&state.node, &signing_key, player, refund_amount).await?;
            let mut game = state.game.lock().await;
            game.refunds.insert(stake_transfer.tx_id, tx_id);
            tx_id
        };
        return Ok(json!({
            "ok": false,
            "refunded": true,
            "refund_tx_id": to_hex(&refund_tx_id),
            "error": "stake amount outside coinflip limits; refund submitted"
        }));
    }

    let mut game = state.game.lock().await;
    let round = game.settle(
        player,
        side,
        client_seed,
        commitment_id,
        stake_transfer.tx_id,
        stake_transfer.amount,
        recent_hash,
    )?;
    Ok(json!({ "ok": true, "round": round.json() }))
}

async fn claim_round(state: &Arc<AppState>, body: &str) -> AppResult<Value> {
    let request: Value = serde_json::from_str(body).map_err(|err| format!("bad claim json: {err}"))?;
    let round_id = decode_hex_32(&required_string(&request, "round_id")?, "round_id")?;
    let claim = {
        let mut game = state.game.lock().await;
        game.prepare_claim(&round_id)?
    };
    let payout_result = sign_and_submit_transfer(
        &state.node,
        &claim.signing_key,
        claim.player,
        claim.payout_amount,
    )
    .await;
    match payout_result {
        Ok(payout_tx_id) => {
            let mut game = state.game.lock().await;
            let round = game.complete_claim(&round_id, payout_tx_id)?;
            Ok(json!({
                "ok": true,
                "payout_tx_id": to_hex(&payout_tx_id),
                "round": round.json()
            }))
        }
        Err(err) => {
            state.game.lock().await.fail_claim(&round_id);
            Err(err)
        }
    }
}

async fn sign_and_submit_transfer(
    node: &NodeClient,
    signing_key: &SigningKey,
    recipient: Pubkey,
    amount: u64,
) -> AppResult<Hash> {
    let signer = signing_key.verifying_key().to_bytes();
    let signer_hex = to_hex(&signer);
    let recipient_hex = to_hex(&recipient);
    let account = node.get_json(&format!("/api/account/{signer_hex}")).await?;
    let status = node.get_json("/api/status").await?;
    let fees = node.get_json("/api/fees").await.unwrap_or_else(|_| json!({}));
    let nonce = string_or_number(&account, "nonce")
        .unwrap_or_else(|| "0".to_string())
        .parse::<u64>()
        .map_err(|_| "bad house nonce".to_string())?
        .checked_add(1)
        .ok_or_else(|| "house nonce overflow".to_string())?;
    let fee = string_or_number(&fees, "recommended_fee")
        .unwrap_or_else(|| "5000".to_string())
        .parse::<u64>()
        .map_err(|_| "bad fee quote".to_string())?;
    let recent_hash = required_string(&status, "recent_block_hash")?;
    let recent_hash_bytes = decode_hex_32(&recent_hash, "recent_block_hash")?;
    let payload = signing_payload(&signer, &recipient, nonce, fee, &recent_hash_bytes, amount);
    let signature = signing_key.sign(&payload).to_bytes();
    let write_set = if signer == recipient {
        signer_hex.clone()
    } else {
        format!("{signer_hex},{recipient_hex}")
    };
    let tx_line = format!(
        "TX {} {} {} {} {} - {} {}:{}:{}",
        signer_hex,
        to_hex(&signature),
        nonce,
        fee,
        recent_hash,
        write_set,
        signer_hex,
        recipient_hex,
        amount
    );
    let queued = node
        .post_text("/api/transactions", &tx_line, "text/plain; charset=utf-8")
        .await?;
    let queued_json: Value = serde_json::from_str(&queued).map_err(|err| format!("bad node submit response: {err}"))?;
    decode_hex_32(&required_string(&queued_json, "tx_id")?, "tx_id")
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CoinSide {
    Heads,
    Tails,
}

impl CoinSide {
    fn parse(value: &str) -> AppResult<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "heads" | "head" | "h" => Ok(Self::Heads),
            "tails" | "tail" | "t" => Ok(Self::Tails),
            _ => Err("coin side must be heads or tails".to_string()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Heads => "heads",
            Self::Tails => "tails",
        }
    }
}

#[derive(Clone)]
struct Commitment {
    id: Hash,
    seed: [u8; 32],
    seed_hash: Hash,
    created_at_ms: u128,
}

impl Commitment {
    fn json(&self) -> Value {
        json!({
            "commitment_id": to_hex(&self.id),
            "server_seed_hash": to_hex(&self.seed_hash),
            "created_at_ms": self.created_at_ms.to_string(),
        })
    }
}

#[derive(Clone)]
struct Round {
    id: Hash,
    player: Pubkey,
    stake_tx_id: Hash,
    stake_amount: u64,
    selected_side: CoinSide,
    outcome_side: CoinSide,
    won: bool,
    payout_amount: u64,
    server_seed: [u8; 32],
    server_seed_hash: Hash,
    client_seed: String,
    entropy: Hash,
    created_at_ms: u128,
    claimed: bool,
    payout_tx_id: Option<Hash>,
}

impl Round {
    fn json(&self) -> Value {
        json!({
            "round_id": to_hex(&self.id),
            "player": to_hex(&self.player),
            "stake_tx_id": to_hex(&self.stake_tx_id),
            "stake_amount": self.stake_amount.to_string(),
            "selected_side": self.selected_side.as_str(),
            "outcome_side": self.outcome_side.as_str(),
            "won": self.won,
            "payout_amount": self.payout_amount.to_string(),
            "server_seed": to_hex(&self.server_seed),
            "server_seed_hash": to_hex(&self.server_seed_hash),
            "client_seed": self.client_seed.clone(),
            "entropy": to_hex(&self.entropy),
            "created_at_ms": self.created_at_ms.to_string(),
            "claimed": self.claimed,
            "payout_tx_id": self.payout_tx_id.map(|tx_id| to_hex(&tx_id)),
        })
    }
}

struct Claim {
    player: Pubkey,
    payout_amount: u64,
    signing_key: SigningKey,
}

struct CoinflipGame {
    signing_key: SigningKey,
    commitments: BTreeMap<Hash, Commitment>,
    commitment_order: VecDeque<Hash>,
    rounds: BTreeMap<Hash, Round>,
    round_order: VecDeque<Hash>,
    refunds: BTreeMap<Hash, Hash>,
}

impl CoinflipGame {
    fn new(house_seed: [u8; 32]) -> AppResult<Self> {
        Ok(Self {
            signing_key: SigningKey::from_bytes(&house_seed),
            commitments: BTreeMap::new(),
            commitment_order: VecDeque::new(),
            rounds: BTreeMap::new(),
            round_order: VecDeque::new(),
            refunds: BTreeMap::new(),
        })
    }

    fn house_pubkey(&self) -> Pubkey {
        self.signing_key.verifying_key().to_bytes()
    }

    fn create_commitment(&mut self) -> AppResult<Commitment> {
        let seed = random_bytes()?;
        let seed_hash = seed_hash(&seed);
        let id = hash_chunks(b"coinflip-commitment", &[&seed_hash, &now_ms().to_le_bytes()]);
        let commitment = Commitment {
            id,
            seed,
            seed_hash,
            created_at_ms: now_ms(),
        };
        self.commitment_order.push_back(id);
        self.commitments.insert(id, commitment.clone());
        while self.commitment_order.len() > MAX_COMMITMENTS {
            if let Some(oldest) = self.commitment_order.pop_front() {
                self.commitments.remove(&oldest);
            }
        }
        Ok(commitment)
    }

    fn settle(
        &mut self,
        player: Pubkey,
        selected_side: CoinSide,
        client_seed: String,
        commitment_id: Hash,
        stake_tx_id: Hash,
        stake_amount: u64,
        recent_block_hash: Hash,
    ) -> AppResult<Round> {
        if self.rounds.values().any(|round| round.stake_tx_id == stake_tx_id) {
            return Err("stake transaction already settled".to_string());
        }
        let commitment = self
            .commitments
            .remove(&commitment_id)
            .ok_or_else(|| "coinflip commitment not found".to_string())?;
        let entropy = hash_chunks(
            b"coinflip-outcome",
            &[
                &commitment.seed,
                client_seed.as_bytes(),
                &stake_tx_id,
                &recent_block_hash,
                &player,
            ],
        );
        let outcome_side = if entropy[31] & 1 == 0 {
            CoinSide::Heads
        } else {
            CoinSide::Tails
        };
        let won = selected_side == outcome_side;
        let payout_amount = if won {
            ((stake_amount as u128) * (PAYOUT_BPS as u128) / 10_000u128) as u64
        } else {
            0
        };
        let id = hash_chunks(
            b"coinflip-round",
            &[&stake_tx_id, &player, &commitment.seed_hash, &entropy],
        );
        let round = Round {
            id,
            player,
            stake_tx_id,
            stake_amount,
            selected_side,
            outcome_side,
            won,
            payout_amount,
            server_seed: commitment.seed,
            server_seed_hash: commitment.seed_hash,
            client_seed,
            entropy,
            created_at_ms: now_ms(),
            claimed: false,
            payout_tx_id: None,
        };
        self.insert_round(round.clone());
        Ok(round)
    }

    fn prepare_claim(&mut self, round_id: &Hash) -> AppResult<Claim> {
        let signing_key = self.signing_key.clone();
        let round = self
            .rounds
            .get_mut(round_id)
            .ok_or_else(|| "coinflip round not found".to_string())?;
        if !round.won {
            return Err("round did not win".to_string());
        }
        if round.claimed {
            return Err("round already claimed".to_string());
        }
        round.claimed = true;
        Ok(Claim {
            player: round.player,
            payout_amount: round.payout_amount,
            signing_key,
        })
    }

    fn complete_claim(&mut self, round_id: &Hash, payout_tx_id: Hash) -> AppResult<Round> {
        let round = self
            .rounds
            .get_mut(round_id)
            .ok_or_else(|| "coinflip round not found".to_string())?;
        round.claimed = true;
        round.payout_tx_id = Some(payout_tx_id);
        Ok(round.clone())
    }

    fn fail_claim(&mut self, round_id: &Hash) {
        if let Some(round) = self.rounds.get_mut(round_id) {
            if round.payout_tx_id.is_none() {
                round.claimed = false;
            }
        }
    }

    fn rounds_json(&self) -> Vec<Value> {
        self.round_order
            .iter()
            .rev()
            .filter_map(|id| self.rounds.get(id))
            .take(50)
            .map(Round::json)
            .collect()
    }

    fn insert_round(&mut self, round: Round) {
        self.round_order.push_back(round.id);
        self.rounds.insert(round.id, round);
        while self.round_order.len() > MAX_ROUNDS {
            if let Some(oldest) = self.round_order.pop_front() {
                self.rounds.remove(&oldest);
            }
        }
    }
}

struct StakeTransfer {
    tx_id: Hash,
    amount: u64,
}

fn extract_stake(tx: &Value, player_hex: &str, house_hex: &str) -> AppResult<StakeTransfer> {
    if tx.get("kind").and_then(Value::as_str) != Some("transfer") {
        return Err("stake transaction is not a transfer".to_string());
    }
    if tx.get("success").and_then(Value::as_bool) != Some(true) {
        return Err("stake transaction did not commit".to_string());
    }
    if required_string(tx, "signer")? != player_hex {
        return Err("stake transaction signer mismatch".to_string());
    }
    let transfers = tx
        .get("transfers")
        .and_then(Value::as_array)
        .ok_or_else(|| "stake transaction has no transfers".to_string())?;
    let transfer = transfers
        .iter()
        .find(|transfer| {
            string_field(transfer, "from").as_deref() == Some(player_hex)
                && string_field(transfer, "to").as_deref() == Some(house_hex)
        })
        .ok_or_else(|| "stake transaction did not pay the coinflip house".to_string())?;
    let amount = required_string(transfer, "amount")?
        .parse::<u64>()
        .map_err(|_| "bad stake amount".to_string())?;
    let tx_id = decode_hex_32(&required_string(tx, "tx_id")?, "tx_id")?;
    Ok(StakeTransfer { tx_id, amount })
}

#[derive(Clone)]
struct NodeClient {
    host: String,
    port: u16,
}

impl NodeClient {
    fn parse(url: &str) -> AppResult<Self> {
        let raw = url
            .strip_prefix("http://")
            .ok_or_else(|| "SYLAS_NODE_URL must be plain http, for example http://sylas:5000".to_string())?;
        let authority = raw.split('/').next().unwrap_or(raw);
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| "SYLAS_NODE_URL must include a port".to_string())?;
        let port = port
            .parse::<u16>()
            .map_err(|_| "SYLAS_NODE_URL has an invalid port".to_string())?;
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }

    async fn get_json(&self, path: &str) -> AppResult<Value> {
        let body = self.request("GET", path, "", "text/plain").await?;
        serde_json::from_str(&body).map_err(|err| format!("bad node json for {path}: {err}"))
    }

    async fn post_text(&self, path: &str, body: &str, content_type: &str) -> AppResult<String> {
        self.request("POST", path, body, content_type).await
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: &str,
        content_type: &str,
    ) -> AppResult<String> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|err| format!("node connect failed: {err}"))?;
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.host,
            self.port,
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|err| format!("node write failed: {err}"))?;
        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .await
            .map_err(|err| format!("node read failed: {err}"))?;
        parse_http_response(&bytes)
    }
}

fn parse_http_response(bytes: &[u8]) -> AppResult<String> {
    let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err("bad http response".to_string());
    };
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(500);
    let body = String::from_utf8_lossy(&bytes[header_end + 4..]).to_string();
    if (200..300).contains(&status) {
        Ok(body)
    } else {
        Err(body)
    }
}

async fn read_request(stream: &mut TcpStream) -> AppResult<Option<HttpRequest>> {
    let mut bytes = Vec::with_capacity(4096);
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|err| format!("http read failed: {err}"))?;
        if read == 0 {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err("incomplete http request".to_string())
            };
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > MAX_HTTP_REQUEST_BYTES {
            return Err("http request too large".to_string());
        }
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
    let content_length = headers
        .lines()
        .skip(1)
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let request_length = header_end + 4 + content_length;
    if request_length > MAX_HTTP_REQUEST_BYTES {
        return Err("http request too large".to_string());
    }
    while bytes.len() < request_length {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|err| format!("http body read failed: {err}"))?;
        if read == 0 {
            return Err("incomplete http body".to_string());
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    let first_line = headers
        .lines()
        .next()
        .ok_or_else(|| "missing http request line".to_string())?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let raw_path = parts.next().unwrap_or("/").to_string();
    let path = raw_path.split('?').next().unwrap_or("/").to_string();
    let body = String::from_utf8_lossy(&bytes[header_end + 4..request_length]).to_string();
    Ok(Some(HttpRequest { method, path, body }))
}

struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: String,
}

impl HttpResponse {
    fn html(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "text/html; charset=utf-8",
            body: body.to_string(),
        }
    }

    fn text(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.to_string(),
        }
    }

    fn json(value: Value) -> Self {
        Self::json_status(200, value)
    }

    fn json_status(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json; charset=utf-8",
            body: value.to_string(),
        }
    }
}

async fn write_response(stream: &mut TcpStream, response: HttpResponse) -> AppResult<()> {
    let status_text = match response.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {} {status_text}\r\nContent-Type: {}\r\nContent-Length: {}\r\nX-Content-Type-Options: nosniff\r\nCache-Control: no-store\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|err| format!("http response write failed: {err}"))?;
    stream
        .write_all(response.body.as_bytes())
        .await
        .map_err(|err| format!("http body write failed: {err}"))
}

fn required_string(value: &Value, field: &str) -> AppResult<String> {
    string_or_number(value, field).ok_or_else(|| format!("{field} is required"))
}

fn string_or_number(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|item| {
        item.as_str()
            .map(str::to_string)
            .or_else(|| item.as_u64().map(|number| number.to_string()))
    })
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn signing_payload(
    signer: &Pubkey,
    recipient: &Pubkey,
    nonce: u64,
    fee: u64,
    recent_hash: &Hash,
    amount: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(180);
    out.extend_from_slice(b"sylas-tx-v1");
    write_u32(&mut out, CHAIN_ID.len() as u32);
    out.extend_from_slice(CHAIN_ID);
    out.extend_from_slice(signer);
    write_u64(&mut out, nonce);
    write_u64(&mut out, fee);
    out.extend_from_slice(recent_hash);
    write_u32(&mut out, 0);
    if signer == recipient {
        write_u32(&mut out, 1);
        out.extend_from_slice(signer);
    } else {
        write_u32(&mut out, 2);
        out.extend_from_slice(signer);
        out.extend_from_slice(recipient);
    }
    write_u32(&mut out, 1);
    out.extend_from_slice(signer);
    out.extend_from_slice(recipient);
    write_u64(&mut out, amount);
    out
}

fn hash_chunks(domain: &[u8], chunks: &[&[u8]]) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(b"sylas-engine-v1:");
    hasher.update((domain.len() as u64).to_le_bytes());
    hasher.update(domain);
    for chunk in chunks {
        hasher.update((chunk.len() as u64).to_le_bytes());
        hasher.update(chunk);
    }
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn seed_hash(seed: &[u8; 32]) -> Hash {
    hash_chunks(b"coinflip-server-seed", &[seed])
}

fn random_bytes() -> AppResult<[u8; 32]> {
    let mut bytes = [0u8; 32];
    getrandom(&mut bytes).map_err(|err| format!("os randomness unavailable: {err}"))?;
    Ok(bytes)
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_hex_32(input: &str, field: &str) -> AppResult<[u8; 32]> {
    let bytes = decode_hex_vec(input)?;
    if bytes.len() != 32 {
        return Err(format!("{field} must be 32 bytes"));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn decode_hex_vec(input: &str) -> AppResult<Vec<u8>> {
    let input = input.trim();
    if input.len() % 2 != 0 {
        return Err("invalid hex".to_string());
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> AppResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex".to_string()),
    }
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

const INDEX_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sylas Coinflip</title>
  <style>
    :root{color-scheme:light;--bg:#f4f7f8;--surface:#fff;--ink:#101820;--muted:#64727d;--line:#d8e1e6;--accent:#087f73;--blue:#245f9c;--ok:#0f7c4f;--danger:#a33b3b;--warn:#9f6b11;--shadow:0 18px 55px rgba(14,29,39,.09)}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;letter-spacing:0}button,input{font:inherit}code,pre{font-family:"Cascadia Mono","SFMono-Regular",Consolas,monospace}
    header{position:sticky;top:0;z-index:5;background:#fff;border-bottom:1px solid var(--line);padding:16px clamp(16px,4vw,52px);display:flex;align-items:center;justify-content:space-between;gap:16px}.brand{display:flex;align-items:center;gap:12px}.mark{width:38px;height:38px;border-radius:8px;background:var(--accent);color:#fff;display:grid;place-items:center;font-weight:950}.eyebrow{margin:0;color:var(--muted);font-size:.73rem;text-transform:uppercase;font-weight:850}h1,h2,p{margin:0;letter-spacing:0}h1{font-size:1.35rem}h2{font-size:1rem}nav{display:flex;gap:8px;flex-wrap:wrap}nav a{border:1px solid var(--line);border-radius:8px;padding:8px 11px;text-decoration:none;color:inherit;font-size:.82rem;font-weight:850;background:#fff}
    main{max-width:1280px;margin:0 auto;padding:18px clamp(14px,3vw,36px) 44px}.hero{display:grid;grid-template-columns:minmax(0,1fr) minmax(330px,.55fr);gap:14px}.copy{display:flex;flex-direction:column;justify-content:center;min-height:245px}.copy h1{font-size:clamp(2rem,5vw,4.2rem);line-height:.98;max-width:820px}.copy p{color:var(--muted);line-height:1.5;margin-top:12px;max-width:720px}
    .panel{background:var(--surface);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow);padding:16px;min-width:0}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:14px}.two{display:grid;grid-template-columns:minmax(0,1.08fr) minmax(360px,.92fr);gap:14px;margin-top:14px}.metric span,.label{display:block;color:var(--muted);font-size:.72rem;font-weight:850;text-transform:uppercase}.metric strong{display:block;margin-top:7px;font-size:1.24rem}.sub{color:var(--muted);font-size:.88rem;line-height:1.45;margin-top:7px}.row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid var(--line)}.row:last-child{border-bottom:0}
    button{border:1px solid var(--line);background:#fff;color:#1b2b34;border-radius:8px;padding:10px 13px;font-weight:850;cursor:pointer}button.primary{background:var(--accent);border-color:var(--accent);color:#fff}button:disabled{opacity:.55;cursor:not-allowed}.segmented{display:grid;grid-template-columns:1fr 1fr;gap:8px}.segmented button.active{background:var(--accent);border-color:var(--accent);color:#fff}.actions{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-top:12px}.wide{width:100%}
    input{width:100%;border:1px solid var(--line);border-radius:8px;background:#fff;color:var(--ink);padding:11px;outline:none;font-size:.86rem}input:focus{border-color:#73aaa2;box-shadow:0 0 0 3px rgba(8,127,115,.12)}.form{display:grid;gap:11px;margin-top:12px}
    .coin-stage{display:grid;place-items:center;min-height:360px;background:linear-gradient(180deg,#fff,#eef5f4);border:1px solid var(--line);border-radius:8px;overflow:hidden}.coin{width:190px;height:190px;border-radius:50%;position:relative;transform-style:preserve-3d;transition:transform .8s cubic-bezier(.2,.8,.2,1);filter:drop-shadow(0 26px 34px rgba(11,35,33,.22))}.coin.flipping{animation:flip 1.45s cubic-bezier(.15,.72,.2,1) infinite}.face{position:absolute;inset:0;border-radius:50%;display:grid;place-items:center;backface-visibility:hidden;border:8px solid #d8b65b;background:radial-gradient(circle at 35% 30%,#fff4ba,#d3a635 70%);color:#3a2c09;font-size:4.5rem;font-weight:950}.tails{transform:rotateY(180deg);background:radial-gradient(circle at 35% 30%,#dff8ff,#5ba8d8 70%);border-color:#75bce4;color:#092f44}.coin.show-tails{transform:rotateY(180deg)}.coin.show-heads{transform:rotateY(0)}@keyframes flip{0%{transform:rotateY(0) rotateX(0)}50%{transform:rotateY(900deg) rotateX(20deg)}100%{transform:rotateY(1800deg) rotateX(0)}}.result{font-size:1.5rem;font-weight:950;margin-top:16px;text-align:center}.ok{color:var(--ok)}.danger{color:var(--danger)}.warn{color:var(--warn)}
    table{width:100%;border-collapse:collapse;font-size:.86rem}th,td{text-align:left;border-bottom:1px solid var(--line);padding:10px;vertical-align:top}th{color:var(--muted);font-size:.72rem;text-transform:uppercase}td code{font-size:.76rem;overflow-wrap:anywhere}.badge{display:inline-flex;border-radius:8px;padding:5px 8px;font-size:.72rem;font-weight:900;text-transform:uppercase}.badge.pass{background:#e7f5ee;color:var(--ok);border:1px solid #b8dfcd}.badge.fail{background:#fae8e8;color:var(--danger);border:1px solid #e6b4b4}pre{margin:0;background:#101816;color:#dceee8;border-radius:8px;padding:14px;overflow:auto;white-space:pre-wrap;min-height:220px}
    @media(max-width:980px){.hero,.grid,.two{grid-template-columns:1fr}header{align-items:flex-start;flex-direction:column}.coin-stage{min-height:290px}.coin{width:150px;height:150px}.face{font-size:3.4rem}}
  </style>
</head>
<body>
  <header>
    <div class="brand"><div class="mark">S</div><div><p class="eyebrow">Sylas Mainnet</p><h1>Coinflip</h1></div></div>
    <nav><a href="/">Game</a><a href="/api/config">Config</a><a href="/api/rounds">Rounds</a></nav>
  </header>
  <main>
    <section class="hero">
      <div class="copy">
        <p class="eyebrow">Isolated SYLAS game service</p>
        <h1>Connect wallet, sign stake, flip, claim.</h1>
        <p>The game never asks for a private key. It uses the Sylas wallet extension to request a signed stake transaction, then settles against a committed server seed.</p>
      </div>
      <div class="panel">
        <h2>Wallet Connection</h2>
        <div class="row"><span class="label">Status</span><strong id="walletStatus">Not connected</strong></div>
        <div class="row"><span class="label">Address</span><code id="walletAddress">-</code></div>
        <div class="row"><span class="label">Balance</span><strong id="walletBalance">-</strong></div>
        <div class="actions"><button class="primary" id="connectWallet">Connect Sylas Wallet</button><button id="refreshWallet">Refresh</button></div>
        <p class="sub" id="walletHint">Install or unlock the Sylas Wallet extension first.</p>
      </div>
    </section>
    <section class="grid">
      <div class="panel metric"><span>House Balance</span><strong id="houseBalance">0</strong></div>
      <div class="panel metric"><span>Min Bet</span><strong id="minBet">0</strong></div>
      <div class="panel metric"><span>Max Bet</span><strong id="maxBet">0</strong></div>
      <div class="panel metric"><span>Payout</span><strong id="payoutRate">0x</strong></div>
    </section>
    <section class="two">
      <div class="panel">
        <div class="coin-stage"><div><div class="coin show-heads" id="coin"><div class="face heads">H</div><div class="face tails">T</div></div><div class="result" id="resultText">Ready</div></div></div>
        <div class="actions"><button class="primary" id="claimButton" disabled>Claim Winnings</button><button id="copyProof" disabled>Copy Fairness Proof</button></div>
        <p class="sub" id="gameStatus">Load config, connect wallet, and place a bet.</p>
      </div>
      <div class="panel">
        <h2>Place Bet</h2>
        <div class="form">
          <label><span class="label">Choose Side</span></label>
          <div class="segmented"><button id="headsButton" class="active" type="button">Heads</button><button id="tailsButton" type="button">Tails</button></div>
          <label><span class="label">Bet Amount</span><input id="betAmount" value="0.01" inputmode="decimal"></label>
          <label><span class="label">Client Seed</span><input id="clientSeed" autocomplete="off" spellcheck="false"></label>
          <div class="row"><span class="label">House Account</span><code id="houseAccount">-</code></div>
          <button class="primary wide" id="placeBet">Sign Stake And Flip</button>
        </div>
      </div>
    </section>
    <section class="two">
      <div class="panel">
        <h2>Recent Rounds</h2>
        <table><thead><tr><th>Round</th><th>Player</th><th>Pick</th><th>Result</th><th>Stake</th><th>Payout</th></tr></thead><tbody id="roundRows"><tr><td colspan="6">No rounds yet.</td></tr></tbody></table>
      </div>
      <div class="panel"><h2>Fairness Proof</h2><pre id="proofJson">{}</pre></div>
    </section>
  </main>
  <script>
    const $=id=>document.getElementById(id);
    let config=null,wallet=null,lastRound=null,selectedSide='heads';
    const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const short=v=>v&&v.length>18?v.slice(0,10)+'...'+v.slice(-8):v||'-';
    const clean=v=>String(v||'').trim().toLowerCase();
    async function api(path,options){const r=await fetch(path,{cache:'no-store',...options});const body=await r.text();const parsed=body?JSON.parse(body):{};if(!r.ok)throw new Error(parsed.error||body);return parsed}
    function nativeUnits(base,decimals=8){const n=BigInt(base||'0'),d=10n**BigInt(decimals),w=n/d,f=(n%d).toString().padStart(decimals,'0').replace(/0+$/,'');return f?w+'.'+f:String(w)}
    function baseUnits(value,decimals=8){const raw=String(value||'0').trim();if(!/^\d+(\.\d+)?$/.test(raw))throw new Error('bad amount');const [whole,frac='']=raw.split('.');if(frac.length>decimals)throw new Error('too many decimals');return BigInt(whole||'0')*(10n**BigInt(decimals))+BigInt((frac+'0'.repeat(decimals)).slice(0,decimals)||'0')}
    function randomSeed(){const b=new Uint8Array(16);crypto.getRandomValues(b);$('clientSeed').value=Array.from(b,x=>x.toString(16).padStart(2,'0')).join('')}
    function setSide(side){selectedSide=side;$('headsButton').classList.toggle('active',side==='heads');$('tailsButton').classList.toggle('active',side==='tails')}
    async function connectWallet(){if(!window.sylas)throw new Error('Sylas Wallet extension provider not found');wallet=await window.sylas.connect();$('walletStatus').textContent='Connected';$('walletAddress').textContent=short(wallet.publicKey);$('walletHint').textContent='Wallet connected through extension provider.';await refreshWallet()}
    async function refreshWallet(){if(!wallet)return;try{const r=await window.sylas.getAccount();$('walletBalance').textContent=nativeUnits(r.balance,config?.token_decimals||8)+' SYLAS'}catch(e){$('walletBalance').textContent='Unavailable'}}
    function animateCoin(outcome){const coin=$('coin');coin.classList.remove('show-heads','show-tails');coin.classList.add('flipping');setTimeout(()=>{coin.classList.remove('flipping');coin.classList.add(outcome==='heads'?'show-heads':'show-tails')},1500)}
    async function settleWithRetry(payload){for(let i=0;i<30;i++){try{return await api('/api/settle',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)})}catch(e){if(!String(e).includes('not found'))throw e;await new Promise(r=>setTimeout(r,500))}}throw new Error('stake transaction not indexed yet')}
    async function placeBet(){try{if(!wallet)await connectWallet();$('placeBet').disabled=true;$('claimButton').disabled=true;const amount=baseUnits($('betAmount').value,config.token_decimals);const min=BigInt(config.min_bet),max=BigInt(config.max_bet);if(amount<min||amount>max)throw new Error('Bet must be between '+nativeUnits(min,config.token_decimals)+' and '+nativeUnits(max,config.token_decimals)+' SYLAS');$('gameStatus').textContent='Requesting committed server seed.';const commit=await api('/api/commit',{method:'POST',headers:{'content-type':'application/json'},body:'{}'});$('gameStatus').textContent='Approve stake in Sylas Wallet.';const stake=await window.sylas.signAndSendTransfer({to:config.house_pubkey,amountBaseUnits:amount.toString(),label:'Sylas Coinflip stake'});$('gameStatus').textContent='Stake queued '+short(stake.tx_id)+'. Waiting for node receipt.';const settled=await settleWithRetry({player:wallet.publicKey,side:selectedSide,client_seed:$('clientSeed').value,stake_tx_id:stake.tx_id,commitment_id:commit.commitment.commitment_id});if(settled.refunded)throw new Error('Stake was invalid and refund was submitted '+short(settled.refund_tx_id));lastRound=settled.round;animateCoin(lastRound.outcome_side);setTimeout(()=>{$('resultText').textContent=lastRound.won?'You won':'You lost';$('resultText').className='result '+(lastRound.won?'ok':'danger');$('claimButton').disabled=!lastRound.won;$('copyProof').disabled=false;$('gameStatus').textContent=lastRound.won?'Claim '+nativeUnits(lastRound.payout_amount,config.token_decimals)+' SYLAS':'Round settled on-chain.'},1550);$('proofJson').textContent=JSON.stringify({commitment:commit.commitment,round:lastRound},null,2);await loadRounds();await refreshWallet()}catch(e){$('gameStatus').textContent=String(e)}finally{$('placeBet').disabled=false}}
    async function claim(){try{if(!lastRound)throw new Error('no winning round');$('claimButton').disabled=true;$('gameStatus').textContent='Submitting house payout.';const result=await api('/api/claim',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({round_id:lastRound.round_id})});lastRound=result.round;$('proofJson').textContent=JSON.stringify(result,null,2);$('gameStatus').textContent='Payout queued '+short(result.payout_tx_id);await loadRounds();await refreshWallet()}catch(e){$('gameStatus').textContent=String(e);$('claimButton').disabled=false}}
    function renderRounds(rounds){if(!rounds.length){$('roundRows').innerHTML='<tr><td colspan="6">No rounds yet.</td></tr>';return}$('roundRows').innerHTML=rounds.map(r=>`<tr><td><code>${esc(short(r.round_id))}</code></td><td><code>${esc(short(r.player))}</code></td><td>${esc(r.selected_side)}</td><td><span class="badge ${r.won?'pass':'fail'}">${esc(r.outcome_side)}</span></td><td>${nativeUnits(r.stake_amount,config?.token_decimals||8)}</td><td>${nativeUnits(r.payout_amount,config?.token_decimals||8)}</td></tr>`).join('')}
    async function loadRounds(){const r=await api('/api/rounds');renderRounds(r.rounds||[])}
    async function load(){config=await api('/api/config');$('houseAccount').textContent=short(config.house_pubkey);$('houseBalance').textContent=nativeUnits(config.house_balance,config.token_decimals);$('minBet').textContent=nativeUnits(config.min_bet,config.token_decimals);$('maxBet').textContent=nativeUnits(config.max_bet,config.token_decimals);$('payoutRate').textContent=(Number(config.payout_bps)/10000).toFixed(2)+'x';await loadRounds();await refreshWallet()}
    $('connectWallet').onclick=()=>connectWallet().catch(e=>$('walletHint').textContent=String(e));$('refreshWallet').onclick=refreshWallet;$('headsButton').onclick=()=>setSide('heads');$('tailsButton').onclick=()=>setSide('tails');$('placeBet').onclick=placeBet;$('claimButton').onclick=claim;$('copyProof').onclick=()=>navigator.clipboard.writeText(JSON.stringify(lastRound,null,2));randomSeed();load().catch(e=>{$('gameStatus').textContent=String(e)});setInterval(()=>load().catch(()=>{}),7000);
  </script>
</body>
</html>"##;
