//! Small, dependency-light utilities.

use sha2::{Digest, Sha256};

use crate::error::{EngineError, EngineResult};

pub type Hash = [u8; 32];

/// Domain-separated SHA-256.
///
/// Each chunk is length-prefixed before hashing so there is no ambiguity between
/// adjacent fields. This keeps transaction and block hashes stable across
/// platforms and compiler versions.
pub fn hash_chunks(domain: &[u8], chunks: &[&[u8]]) -> Hash {
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

pub fn hash_bytes(domain: &[u8], bytes: &[u8]) -> Hash {
    hash_chunks(domain, &[bytes])
}

pub fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn decode_hex<const N: usize>(input: &str, field: &'static str) -> EngineResult<[u8; N]> {
    let raw = decode_hex_vec(input)?;
    if raw.len() != N {
        return Err(EngineError::InvalidLength {
            field,
            expected: N,
            actual: raw.len(),
        });
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&raw);
    Ok(out)
}

pub fn decode_hex_vec(input: &str) -> EngineResult<Vec<u8>> {
    let input = input.trim();
    if input.len() % 2 != 0 {
        return Err(EngineError::InvalidHex);
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> EngineResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(EngineError::InvalidHex),
    }
}

pub fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
