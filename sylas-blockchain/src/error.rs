//! Error types shared across the engine.
//!
//! The engine intentionally avoids stringly typed errors in core paths. The
//! variants below preserve enough information to operate the node and diagnose
//! bad transactions without leaking implementation details to clients.

use std::{error::Error, fmt, io};

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Debug)]
pub enum EngineError {
    Io(io::Error),
    InvalidHex,
    InvalidLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    InvalidSignature,
    InvalidTransaction(&'static str),
    StateFull,
    AccountNotFound,
    InsufficientFunds,
    ArithmeticOverflow,
    StorageCorruption(&'static str),
    SchedulerBuild(String),
    Network(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::InvalidHex => write!(f, "invalid hex encoding"),
            Self::InvalidLength {
                field,
                expected,
                actual,
            } => write!(f, "{field} has invalid length: expected {expected}, got {actual}"),
            Self::InvalidSignature => write!(f, "invalid transaction signature"),
            Self::InvalidTransaction(reason) => write!(f, "invalid transaction: {reason}"),
            Self::StateFull => write!(f, "state database is full"),
            Self::AccountNotFound => write!(f, "account not found"),
            Self::InsufficientFunds => write!(f, "insufficient funds"),
            Self::ArithmeticOverflow => write!(f, "arithmetic overflow"),
            Self::StorageCorruption(reason) => write!(f, "state storage corruption: {reason}"),
            Self::SchedulerBuild(reason) => write!(f, "scheduler build error: {reason}"),
            Self::Network(reason) => write!(f, "network error: {reason}"),
        }
    }
}

impl Error for EngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for EngineError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
