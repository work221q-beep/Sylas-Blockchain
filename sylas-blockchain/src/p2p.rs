//! Lightweight peer service.
//!
//! This is the first distributed-systems layer for Sylas: node identity,
//! peer discovery, status exchange, and bootstrap dialing. Consensus messages
//! can build on this same transport once the validator protocol hardens.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::{timeout, Duration},
};

use crate::{
    error::{EngineError, EngineResult},
    executor::Engine,
    util::to_hex,
};

pub type SharedPeerBook = Arc<Mutex<PeerBook>>;

#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub node_id: String,
    pub address: String,
    pub height: u64,
    pub recent_block_hash: String,
    pub last_seen_ms: u128,
}

#[derive(Default)]
pub struct PeerBook {
    peers: BTreeMap<String, PeerInfo>,
}

impl PeerBook {
    pub fn upsert(&mut self, peer: PeerInfo) {
        self.peers.insert(peer.node_id.clone(), peer);
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn list(&self) -> Vec<PeerInfo> {
        self.peers.values().cloned().collect()
    }
}

pub async fn run_p2p_service(
    bind: String,
    node_id: String,
    public_addr: String,
    engine: Arc<Mutex<Engine>>,
    peer_book: SharedPeerBook,
) -> EngineResult<()> {
    let listener = TcpListener::bind(&bind).await?;
    println!("Sylas P2P service listening on {bind}");
    loop {
        let (stream, _) = listener.accept().await?;
        let node_id = node_id.clone();
        let public_addr = public_addr.clone();
        let engine = engine.clone();
        let peer_book = peer_book.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_peer(stream, node_id, public_addr, engine, peer_book).await
            {
                eprintln!("p2p connection error: {err}");
            }
        });
    }
}

pub async fn run_bootstrap_connector(
    peers: Vec<String>,
    node_id: String,
    public_addr: String,
    engine: Arc<Mutex<Engine>>,
    peer_book: SharedPeerBook,
) {
    if peers.is_empty() {
        return;
    }
    loop {
        for peer_addr in &peers {
            if let Err(err) = dial_peer(
                peer_addr,
                &node_id,
                &public_addr,
                engine.clone(),
                peer_book.clone(),
            )
            .await
            {
                eprintln!("bootstrap peer {peer_addr} unavailable: {err}");
            }
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

pub fn peers_json(peers: &[PeerInfo]) -> String {
    let body = peers
        .iter()
        .map(|peer| {
            format!(
                r#"{{"node_id":"{}","address":"{}","height":{},"recent_block_hash":"{}","last_seen_ms":{}}}"#,
                json_escape(&peer.node_id),
                json_escape(&peer.address),
                peer.height,
                json_escape(&peer.recent_block_hash),
                peer.last_seen_ms
            )
        })
        .collect::<Vec<String>>()
        .join(",");
    format!(r#"{{"peers":[{}]}}"#, body)
}

fn parse_bootstrap_peers(spec: &str) -> Vec<String> {
    spec.split(',')
        .map(str::trim)
        .filter(|peer| !peer.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub fn bootstrap_peers(spec: &str) -> Vec<String> {
    parse_bootstrap_peers(spec)
}

async fn handle_peer(
    stream: TcpStream,
    node_id: String,
    public_addr: String,
    engine: Arc<Mutex<Engine>>,
    peer_book: SharedPeerBook,
) -> EngineResult<()> {
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(format!("SYLAS P2P/1.0 {}\n", node_id).as_bytes())
        .await?;
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(peer) = parse_hello(line) {
            if peer.node_id != node_id {
                peer_book.lock().await.upsert(peer);
            }
            writer.write_all(b"OK\n").await?;
        } else if line == "STATUS" {
            writer
                .write_all(status_line(&node_id, &public_addr, engine.clone()).await.as_bytes())
                .await?;
        } else if line == "PEERS" {
            let peers = peer_book.lock().await.list();
            for peer in peers {
                writer
                    .write_all(
                        format!(
                            "PEER {} {} {} {} {}\n",
                            peer.node_id,
                            peer.address,
                            peer.height,
                            peer.recent_block_hash,
                            peer.last_seen_ms
                        )
                        .as_bytes(),
                    )
                    .await?;
            }
            writer.write_all(b"END\n").await?;
        } else {
            writer.write_all(b"ERR unknown command\n").await?;
        }
    }
    Ok(())
}

async fn dial_peer(
    peer_addr: &str,
    node_id: &str,
    public_addr: &str,
    engine: Arc<Mutex<Engine>>,
    peer_book: SharedPeerBook,
) -> EngineResult<()> {
    let stream = timeout(Duration::from_secs(5), TcpStream::connect(peer_addr))
        .await
        .map_err(|_| EngineError::Network("p2p dial timeout".into()))??;
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(status_line(node_id, public_addr, engine).await.as_bytes())
        .await?;
    writer.write_all(b"STATUS\n").await?;
    writer.write_all(b"PEERS\n").await?;

    let mut lines = BufReader::new(reader).lines();
    loop {
        match timeout(Duration::from_secs(3), lines.next_line()).await {
            Ok(Ok(Some(line))) => {
                let line = line.trim();
                if line == "END" {
                    break;
                }
                if let Some(peer) = parse_peer(line) {
                    if peer.node_id != node_id {
                        peer_book.lock().await.upsert(peer);
                    }
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => break,
        }
    }
    Ok(())
}

async fn status_line(
    node_id: &str,
    public_addr: &str,
    engine: Arc<Mutex<Engine>>,
) -> String {
    let engine = engine.lock().await;
    format!(
        "HELLO {} {} {} {}\n",
        node_id,
        public_addr,
        engine.height(),
        to_hex(&engine.recent_block_hash())
    )
}

fn parse_hello(line: &str) -> Option<PeerInfo> {
    let parts = line.split_whitespace().collect::<Vec<&str>>();
    if parts.len() != 5 || parts[0] != "HELLO" {
        return None;
    }
    Some(PeerInfo {
        node_id: parts[1].to_string(),
        address: parts[2].to_string(),
        height: parts[3].parse::<u64>().ok()?,
        recent_block_hash: parts[4].to_string(),
        last_seen_ms: now_ms(),
    })
}

fn parse_peer(line: &str) -> Option<PeerInfo> {
    let parts = line.split_whitespace().collect::<Vec<&str>>();
    if parts.len() != 6 || parts[0] != "PEER" {
        return parse_hello(line);
    }
    Some(PeerInfo {
        node_id: parts[1].to_string(),
        address: parts[2].to_string(),
        height: parts[3].parse::<u64>().ok()?,
        recent_block_hash: parts[4].to_string(),
        last_seen_ms: parts[5].parse::<u128>().ok().unwrap_or_else(now_ms),
    })
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
