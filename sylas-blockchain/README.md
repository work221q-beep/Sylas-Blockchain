# Sylas Blockchain

Rust-only blockchain node with an integrated web console, explorer,
market/supply surface, and signed transaction ingress.

Implemented exactly around the requested stack:

- `tokio` async HTTP and TCP networking
- `rayon` parallel transaction execution
- `memmap2` memory-mapped state storage
- `sha2` deterministic hashing
- Ed25519 transaction signatures
- Append-only persistent block ledger
- Explicit `read_set` and `write_set` transaction access declarations
- Dependency/conflict scheduler for deterministic parallel execution
- Browser wallet using local Ed25519 signing through WebCrypto
- Account, block, transaction explorer, fee quote, airdrop, valuation, and transaction submission APIs
- Alpha P2P peer discovery and validator-set/quorum status
- Built-in diagnostics button and `GET /api/debug` self-test endpoint
- Built-in explorer for recent transactions, transaction hash lookup, and account history
- Durable append-only receipt log for transaction history across redeploys
- Isolated coinflip dapp service in `apps/coinflip/`
- Unpacked browser wallet extension in `extension/` with dapp provider signing
- Hard-capped maximum supply: `1,000,000 SYLAS`
- Manual wallet airdrop: `1 SYLAS` every 24 hours, capped by total supply

## Mainnet Status

The default deployment runs a single Sylas node. Alpha peer discovery and
validator quorum accounting are included, but a real public mainnet still
requires multiple independently operated validators, audited consensus finality,
public RPC infrastructure, monitoring, incident response, exchange/indexer
integrations, and published token economics.

The console will show market cap only when `SYLAS_TOKEN_PRICE_USD` is supplied
from an external market source. Without a traded price, market cap is correctly
reported as unavailable instead of fabricated.

The maximum supply is hardcoded to:

```text
1,000,000 SYLAS
```

Initial owned supply is `0 / 1,000,000 SYLAS` unless you explicitly configure
genesis allocations or a wallet manually claims the daily airdrop.

On redeploys, existing Docker volumes keep prior balances, block headers, and
transaction receipts. Clear the old `/var/lib/sylas` volume only if you need a
fresh zero-supply chain.

## Ports

- Web dashboard/API: `5000`
- Raw TCP transaction ingress: `7077`
- P2P peer service: `9090`

In Dokploy, set the domain container port to:

```text
5000
```

## Docker

```bash
docker compose up -d --build
```

Open:

```text
http://localhost:5000
```

Standalone explorer:

```text
http://localhost:5000/explorer
```

Isolated coinflip app:

```text
apps/coinflip
```

## Browser Extension

Load `extension/` as an unpacked Chrome or Edge extension. The extension opens
with a create/import wallet flow, stores keys locally, uses the hosted Sylas
node at `https://web3.sylas.lol`, signs transfers locally, and includes network
diagnostics for the wallet-facing HTTP endpoints. Dapps use the injected
`window.sylas` provider to request connect/account/sign actions without seeing
private key material.

## Isolated Coinflip App

The coinflip game lives outside the core node under `apps/coinflip/`. Deploy it
as a separate Dokploy app/container and point it at the Sylas node API.

Required coinflip env:

```text
SYLAS_COINFLIP_BIND=0.0.0.0:5080
SYLAS_NODE_URL=http://sylas:5000
SYLAS_COINFLIP_HOUSE_SECRET_HEX=32_BYTE_HEX_SEED
```

The game preflights the configured min/max bet before asking the extension to
sign. If a user bypasses the UI and sends an out-of-range stake to the house,
the service attempts to refund the stake minus the current network fee instead
of settling the bet.

## Dokploy Environment

No env file is required in the zip. Configure variables in Dokploy only if you need overrides:

```text
SYLAS_HTTP_BIND=0.0.0.0:5000
SYLAS_TX_BIND=0.0.0.0:7077
SYLAS_P2P_BIND=0.0.0.0:9090
SYLAS_PUBLIC_P2P_ADDR=
SYLAS_NODE_ID=
SYLAS_BOOTSTRAP_PEERS=
SYLAS_VALIDATORS=
SYLAS_STATE_PATH=/var/lib/sylas/state.mmap
SYLAS_LEDGER_PATH=/var/lib/sylas/ledger.dat
SYLAS_RECEIPTS_PATH=/var/lib/sylas/receipts.log
SYLAS_STATE_CAPACITY=65536
SYLAS_MAX_BATCH=1024
SYLAS_GENESIS_ACCOUNTS=
SYLAS_TOKEN_PRICE_USD=
```

`SYLAS`, 8 decimals, and the `1,000,000 SYLAS` cap are fixed in code so the
node and wallet cannot disagree.

`SYLAS_GENESIS_ACCOUNTS` format:

```text
PUBKEY_HEX:BALANCE,PUBKEY_HEX:BALANCE
```

`SYLAS_VALIDATORS` format:

```text
NODE_ID:VOTING_POWER,NODE_ID:VOTING_POWER
```

`SYLAS_BOOTSTRAP_PEERS` format:

```text
HOST:9090,HOST:9090
```

## Zip Rules

Zip the contents of this folder so `Dockerfile` and `Cargo.toml` are at the zip root.

Include:

- `src`
- `docs`
- `extension`
- `apps`
- `.dockerignore`
- `Cargo.toml`
- `docker-compose.yml`
- `docker-compose.dokploy.yml`
- `Dockerfile`
- `README.md`
- `SECURITY.md`

Do not include:

- `.env`
- `data`
- `target`
- compiled binaries
- old zip files
