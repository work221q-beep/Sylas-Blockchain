//! Alpha consensus state and validator-set accounting.
//!
//! This module does not pretend to be production BFT finality. It gives the
//! node a real validator identity, quorum math, and consensus status surface so
//! multi-node work can be built without lying about mainnet readiness.

use std::collections::BTreeMap;

use crate::{
    types::Block,
    util::{Hash, to_hex},
};

#[derive(Clone, Debug)]
pub struct Validator {
    pub id: String,
    pub voting_power: u64,
}

#[derive(Clone, Debug)]
pub struct ConsensusConfig {
    pub node_id: String,
    pub validators: Vec<Validator>,
}

#[derive(Clone, Debug)]
pub struct ConsensusSnapshot {
    pub node_id: String,
    pub mode: &'static str,
    pub status: &'static str,
    pub validator_count: usize,
    pub total_power: u64,
    pub own_power: u64,
    pub quorum_power: u64,
    pub latest_height: u64,
    pub latest_hash: Hash,
    pub latest_state_root: Hash,
    pub latest_tx_count: usize,
    pub latest_timestamp_ms: u128,
}

pub struct ConsensusEngine {
    config: ConsensusConfig,
    latest_block: Option<Block>,
}

impl ConsensusEngine {
    pub fn new(config: ConsensusConfig) -> Self {
        Self {
            config,
            latest_block: None,
        }
    }

    pub fn record_commit(&mut self, block: &Block) {
        self.latest_block = Some(block.clone());
    }

    pub fn snapshot(&self, recent_block_hash: Hash, state_root: Hash) -> ConsensusSnapshot {
        let total_power = self.total_power();
        let own_power = self.own_power();
        let quorum_power = quorum_power(total_power);
        let validator_count = self.config.validators.len();
        let mode = if validator_count <= 1 {
            "single-validator"
        } else {
            "multi-validator-alpha"
        };
        let status = if validator_count <= 1 {
            "local-finality"
        } else {
            "quorum-accounting"
        };

        if let Some(block) = &self.latest_block {
            ConsensusSnapshot {
                node_id: self.config.node_id.clone(),
                mode,
                status,
                validator_count,
                total_power,
                own_power,
                quorum_power,
                latest_height: block.height,
                latest_hash: block.hash,
                latest_state_root: block.state_root,
                latest_tx_count: block.tx_count,
                latest_timestamp_ms: block.timestamp_ms,
            }
        } else {
            ConsensusSnapshot {
                node_id: self.config.node_id.clone(),
                mode,
                status,
                validator_count,
                total_power,
                own_power,
                quorum_power,
                latest_height: 0,
                latest_hash: recent_block_hash,
                latest_state_root: state_root,
                latest_tx_count: 0,
                latest_timestamp_ms: 0,
            }
        }
    }

    pub fn validators(&self) -> &[Validator] {
        &self.config.validators
    }

    fn total_power(&self) -> u64 {
        self.config
            .validators
            .iter()
            .map(|validator| validator.voting_power)
            .sum()
    }

    fn own_power(&self) -> u64 {
        self.config
            .validators
            .iter()
            .find(|validator| validator.id == self.config.node_id)
            .map_or(0, |validator| validator.voting_power)
    }
}

pub fn consensus_config(node_id: String, validators_spec: &str) -> ConsensusConfig {
    let mut merged = BTreeMap::new();
    for validator in parse_validators(validators_spec) {
        let power = merged.entry(validator.id).or_insert(0u64);
        *power = power.saturating_add(validator.voting_power);
    }
    if merged.is_empty() || !merged.contains_key(&node_id) {
        merged.insert(node_id.clone(), 1);
    }
    let validators = merged
        .into_iter()
        .map(|(id, voting_power)| Validator { id, voting_power })
        .collect();
    ConsensusConfig {
        node_id,
        validators,
    }
}

pub fn validator_json(validators: &[Validator]) -> String {
    let body = validators
        .iter()
        .map(|validator| {
            format!(
                r#"{{"id":"{}","voting_power":{}}}"#,
                json_escape(&validator.id),
                validator.voting_power
            )
        })
        .collect::<Vec<String>>()
        .join(",");
    format!(r#"{{"validators":[{}]}}"#, body)
}

pub fn snapshot_json(snapshot: &ConsensusSnapshot) -> String {
    format!(
        r#"{{
  "node_id": "{}",
  "mode": "{}",
  "status": "{}",
  "validator_count": {},
  "total_power": {},
  "own_power": {},
  "quorum_power": {},
  "latest_height": {},
  "latest_hash": "{}",
  "latest_state_root": "{}",
  "latest_tx_count": {},
  "latest_timestamp_ms": {}
}}"#,
        json_escape(&snapshot.node_id),
        snapshot.mode,
        snapshot.status,
        snapshot.validator_count,
        snapshot.total_power,
        snapshot.own_power,
        snapshot.quorum_power,
        snapshot.latest_height,
        to_hex(&snapshot.latest_hash),
        to_hex(&snapshot.latest_state_root),
        snapshot.latest_tx_count,
        snapshot.latest_timestamp_ms
    )
}

fn parse_validators(spec: &str) -> Vec<Validator> {
    spec.split(',')
        .filter_map(|raw| {
            let raw = raw.trim();
            if raw.is_empty() {
                return None;
            }
            let (id, power) = raw.split_once(':')?;
            let id = id.trim();
            if id.is_empty() {
                return None;
            }
            let voting_power = power.trim().parse::<u64>().ok()?.max(1);
            Some(Validator {
                id: id.to_string(),
                voting_power,
            })
        })
        .collect()
}

fn quorum_power(total_power: u64) -> u64 {
    total_power.saturating_mul(2) / 3 + 1
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
