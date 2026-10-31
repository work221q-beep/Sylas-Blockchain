//! Tokio TCP ingress.
//!
//! The server accepts line-delimited transactions and applies backpressure via
//! a bounded channel. Parsed transactions are not executed on socket tasks;
//! they are streamed into the verification and scheduling pipeline.

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

use crate::{
    error::{EngineError, EngineResult},
    types::{Pubkey, Transaction, TransferOp},
    util::decode_hex,
};

pub async fn run_tcp_ingress(bind: String, sender: mpsc::Sender<Transaction>) -> EngineResult<()> {
    let listener = TcpListener::bind(&bind).await?;
    println!("Sylas TCP ingress listening on {bind}");
    loop {
        let (stream, peer) = listener.accept().await?;
        let sender = sender.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, sender).await {
                eprintln!("connection {peer} closed with error: {err}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    sender: mpsc::Sender<Transaction>,
) -> EngineResult<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    writer
        .write_all(b"SYLAS TCP/1.0 ready. Send line-delimited TX records.\n")
        .await?;
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match parse_transaction_line(line) {
            Ok(tx) => {
                sender
                    .send(tx)
                    .await
                    .map_err(|_| EngineError::Network("execution pipeline is closed".into()))?;
                writer.write_all(b"QUEUED\n").await?;
            }
            Err(err) => {
                writer
                    .write_all(format!("REJECTED {err}\n").as_bytes())
                    .await?;
            }
        }
    }
    Ok(())
}

/// Parses the compact text wire format:
///
/// `TX <pubkey> <signature> <nonce> <fee> <recent_hash> <read_csv> <write_csv> <ops>`
///
/// Empty read/write CSV fields are represented by `-`.
pub fn parse_transaction_line(line: &str) -> EngineResult<Transaction> {
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() != 9 || fields[0] != "TX" {
        return Err(EngineError::InvalidTransaction(
            "expected: TX <pubkey> <signature> <nonce> <fee> <recent_hash> <read_csv> <write_csv> <ops>",
        ));
    }
    let signer = decode_hex::<32>(fields[1], "signer")?;
    let signature = decode_hex::<64>(fields[2], "signature")?;
    let nonce = fields[3]
        .parse::<u64>()
        .map_err(|_| EngineError::InvalidTransaction("bad nonce"))?;
    let fee = fields[4]
        .parse::<u64>()
        .map_err(|_| EngineError::InvalidTransaction("bad fee"))?;
    let recent_block_hash = decode_hex::<32>(fields[5], "recent_block_hash")?;
    let read_set = parse_pubkey_csv(fields[6])?;
    let write_set = parse_pubkey_csv(fields[7])?;
    let transfers = parse_transfer_ops(fields[8])?;
    Ok(Transaction {
        signer,
        signature,
        nonce,
        fee,
        recent_block_hash,
        read_set,
        write_set,
        transfers,
    })
}

fn parse_pubkey_csv(input: &str) -> EngineResult<Vec<Pubkey>> {
    if input == "-" {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|part| decode_hex::<32>(part, "pubkey"))
        .collect()
}

fn parse_transfer_ops(input: &str) -> EngineResult<Vec<TransferOp>> {
    input
        .split(',')
        .map(|part| {
            let fields: Vec<&str> = part.split(':').collect();
            if fields.len() != 3 {
                return Err(EngineError::InvalidTransaction("bad transfer op"));
            }
            let from = decode_hex::<32>(fields[0], "transfer.from")?;
            let to = decode_hex::<32>(fields[1], "transfer.to")?;
            let amount = fields[2]
                .parse::<u64>()
                .map_err(|_| EngineError::InvalidTransaction("bad transfer amount"))?;
            Ok(TransferOp { from, to, amount })
        })
        .collect()
}
