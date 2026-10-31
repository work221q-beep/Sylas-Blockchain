# Architecture

Sylas is a Rust-only minimal blockchain node.

Core components:

- `types.rs`: `Transaction`, `Account`, `StateEntry`, `Block`
- `scheduler.rs`: dependency graph scheduler based on declared write sets
- `executor.rs`: signature verification and Rayon parallel execution
- `storage.rs`: `memmap2` state file with fixed-size account slots
- `ledger.rs`: append-only persistent block header ledger
- `network.rs`: Tokio TCP transaction ingress
- `p2p.rs`: Tokio peer service and bootstrap peer exchange
- `consensus.rs`: validator identity, voting power, and quorum accounting
- `http.rs`: Tokio HTTP dashboard, explorer, and API on port `5000`
- `receipts.rs`: append-only transaction receipt log for explorer history
- `extension/`: Chrome/Edge browser wallet, endpoint test harness, and dapp provider
- `apps/coinflip/`: isolated Rust coinflip service that uses the Sylas API

Transactions declare:

- `read_set`
- `write_set`

Transactions with disjoint `write_set` values execute concurrently. Transactions with write conflicts are placed into later deterministic stages.

Restart safety:

- Account state persists in `SYLAS_STATE_PATH`.
- Block height and previous block hash persist in `SYLAS_LEDGER_PATH`.
- Transaction receipts persist in `SYLAS_RECEIPTS_PATH`.
- On startup, the engine restores its chain head from the append-only ledger and rejects startup if the current state root does not match the persisted ledger head.
- The explorer restores recent receipts from the append-only receipt log.

Public API surfaces:

- `GET /explorer`: standalone Solscan-style Sylas explorer page
- `GET /api/status`: node health, roots, scheduler stats, and economics summary
- `GET /api/debug`: pass/warn/fail diagnostics for storage, ledger, execution, consensus, P2P, transaction queue, and economics config
- `GET /api/consensus`: validator mode, quorum power, and latest committed block
- `GET /api/validators`: configured validator set
- `GET /api/peers`: connected peer book
- `GET /api/economics`: supply, token display settings, optional valuation
- `GET /api/fees`: variable node-congestion fee recommendation
- `GET /api/accounts`: top account balances
- `GET /api/account/<pubkey>`: account balance, nonce, version, and data
- `GET /api/blocks`: recent committed blocks
- `GET /api/transactions`: recent transaction receipts retained by this node
- `GET /api/transaction/<hash>`: transaction receipt lookup
- `GET /api/account/<pubkey>/transactions`: account transaction history
- `POST /api/airdrop`: manual `1 SYLAS` wallet claim, limited to once every 24 hours per account
- `POST /api/transactions`: signed line-delimited `TX ...` submission
- `POST /api/tokens/create`: reserved secondary token/memecoin creation endpoint
- `POST /api/tokens/close-account`: reserved secondary token account close endpoint
- `POST /api/swap/quote`: reserved swap quote endpoint
- `POST /api/bridge/quote`: reserved bridge quote endpoint

Isolated coinflip service API:

- `GET /`: animated coinflip dapp page
- `GET /api/config`: house account, limits, payout terms, fee quote, and node status
- `GET /api/rounds`: recent game rounds retained by the coinflip service
- `POST /api/commit`: reserve a per-bet server seed commitment before wallet signing
- `POST /api/settle`: settle a confirmed house stake transaction or refund invalid stake sizes
- `POST /api/claim`: submit a winning payout from the house wallet

Wallet extension:

- Uses the hosted Sylas Mainnet node at `https://web3.sylas.lol`.
- Starts with create/import onboarding before showing wallet balances.
- Creates/imports Ed25519 wallets with WebCrypto.
- Stores keys in extension-local storage.
- Signs Sylas transaction payloads locally.
- Injects `window.sylas` so dapps can connect and request signatures without seeing private keys.
- Provides Send, Receive, QR code, manual Airdrop, fee quote, and diagnostics screens.
- Tests wallet-facing HTTP endpoints, including explorer endpoints, and marks pass/warn/fail results.
- Enforces the displayed maximum token cap of `1,000,000 SYLAS`.
- Uses 8 decimal places for base-unit conversion.
