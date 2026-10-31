# Threat Model

Current defenses focus on local execution correctness:

- Invalid signatures are rejected.
- Transactions are bound to a recent block hash.
- Nonces prevent replay.
- Declared write sets drive deterministic conflict scheduling.
- Balance updates use checked arithmetic.
- State roots commit the memory-mapped account state.
- Block headers persist in an append-only ledger and are hash-checked on recovery.
- Peer discovery records remote node IDs, advertised addresses, heights, and recent hashes.
- Validator quorum math is exposed for operations review.
- Runtime diagnostics expose pass/warn/fail status for storage, ledger, execution, consensus, P2P, queue, and market configuration.
- Airdrop minting is rate-limited per account and checked against the 1,000,000 SYLAS cap.
- Transaction receipts persist in an append-only log so explorer history survives node restarts when `/var/lib/sylas` is persistent.
- The coinflip dapp is isolated from the node process and talks to Sylas through the public API.
- Coinflip settlement reveals the per-bet committed server seed so players can recompute the outcome from the server seed, client seed, stake transaction hash, and chain head.
- Coinflip preflights bet limits before wallet signing and attempts an automatic refund for out-of-range stakes submitted outside the UI.
- Dapps request signatures through the Sylas Wallet provider and never receive private keys.

Current gaps:

- No audited public validator consensus.
- No slashing.
- P2P is alpha peer/status exchange, not a hardened adversarial network layer.
- No privacy layer.
- Coinflip house bankroll/key management is operator-controlled and must be funded, monitored, and deployed separately from validator nodes.
- No audit.
