# Sylas Security Policy

Sylas is alpha node software. Do not use it for real funds until consensus,
operations, and external audits are complete.

Implemented protections:

- Ed25519 transaction signatures
- Chain ID in the signing payload
- Recent block hash replay protection
- Account nonces
- Explicit read/write account declarations
- Deterministic write-conflict scheduling
- Checked balance arithmetic
- Memory-mapped state persistence through `memmap2`
- Append-only block header ledger for restart-safe chain head recovery
- Append-only transaction receipt log for explorer recovery after restart
- Domain-separated `sha2` hashing
- Browser wallet signs locally and never sends private keys to the node
- Browser extension stores keys in extension-local storage and signs locally
- Browser extension exposes a dapp provider so websites can request signatures without receiving private keys
- Maximum supply is capped at 1,000,000 SYLAS for genesis allocation and airdrop minting
- Manual airdrop is limited to 1 SYLAS every 24 hours per wallet account
- Isolated coinflip outcomes use a per-bet server seed commitment, client seed, stake transaction hash, and chain head entropy
- Isolated coinflip payouts require a house signing seed configured outside the zip through deployment environment
- Isolated coinflip preflights bet limits and attempts refunds for out-of-range stakes
- Alpha peer discovery and validator quorum accounting

Not implemented yet:

- Audited Byzantine fault tolerant consensus
- Validator slashing
- Production key management
- Smart contract sandboxing
- Privacy circuits
- External audit
