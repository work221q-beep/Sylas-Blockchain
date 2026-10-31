# Dokploy Deployment

Sylas is now a Rust-only node. The web dashboard/API listens on container port `5000`.
The optional peer service listens on container port `9090`.

The standalone explorer is served by the same container at:

```text
/explorer
```

The coinflip game is intentionally not served by the node container. Deploy
`apps/coinflip/` as a separate Dokploy app/container and connect it to the node
through `SYLAS_NODE_URL`.

## Domain Port

Set the Dokploy domain port to:

```text
5000
```

## Zip Upload

Create the zip from inside `sylas-blockchain` so these files are at the zip root:

- `Dockerfile`
- `Cargo.toml`
- `src/`
- `extension/`
- `docker-compose.dokploy.yml`

Do not include env files, `target/`, `data/`, or compiled binaries.

## Dokploy Settings

Use Dockerfile build mode or Docker Compose with:

```text
docker-compose.dokploy.yml
```

Environment variables are optional because Dockerfile and Compose include defaults:

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

The primary token rules are fixed in code: `SYLAS`, 8 decimals, and a
`1,000,000 SYLAS` maximum supply.

Use `SYLAS_GENESIS_ACCOUNTS` only when you want to pre-fund accounts. Leave it
empty for the default `0 / 1,000,000 SYLAS` owned supply:

```text
PUBKEY_HEX:BALANCE,PUBKEY_HEX:BALANCE
```

For multi-node alpha testing, set each node's public peer address and validator
identity in Dokploy:

```text
SYLAS_PUBLIC_P2P_ADDR=node-a.example.com:9090
SYLAS_NODE_ID=node-a
SYLAS_BOOTSTRAP_PEERS=node-b.example.com:9090,node-c.example.com:9090
SYLAS_VALIDATORS=node-a:1,node-b:1,node-c:1
```

Expose `9090/tcp` only between validators or behind firewall rules. Keep the
public website/domain on `5000`.

`SYLAS_TOKEN_PRICE_USD` is optional. Set it only if you have an external price
source; the node will then calculate market cap from live supply. If it is not
set, the console shows valuation as unavailable.

Persist for the node:

```text
/var/lib/sylas
```

If you redeploy over an older test chain and need the owned supply to return to
`0 / 1,000,000 SYLAS`, remove the old Dokploy/Docker volume for `/var/lib/sylas`
before starting the new container. Keeping the volume keeps the previous chain
state by design.

## Health Check

After deployment:

```bash
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs --tail=100 CONTAINER_NAME
docker exec -it CONTAINER_NAME wget -qO- http://127.0.0.1:5000/healthz
docker exec -it CONTAINER_NAME wget -qO- http://127.0.0.1:5000/api/debug
```

Expected:

```text
ok
```

Open the explorer at:

```text
https://YOUR_DOMAIN/explorer
```

## Isolated Coinflip Deployment

Create a separate Dokploy app using:

```text
Dockerfile path: apps/coinflip/Dockerfile
Container port: 5080
```

Set these env vars in that coinflip app:

```text
SYLAS_COINFLIP_BIND=0.0.0.0:5080
SYLAS_NODE_URL=http://sylas:5000
SYLAS_COINFLIP_HOUSE_SECRET_HEX=32_BYTE_HEX_SEED
```

Use a separate domain such as `coinflip.YOUR_DOMAIN`. The coinflip service
uses the Sylas API and the Sylas Wallet extension provider; it does not ask
players to paste private keys.

## Wallet Extension

After deployment, load `extension/` as an unpacked Chrome or Edge extension. The
extension is hardwired to the hosted Sylas node:

```text
https://web3.sylas.lol
```

Use the extension's endpoint panel to test `/healthz`, `/api/status`,
`/api/debug`, `/api/consensus`, `/api/validators`, `/api/peers`,
`/api/blocks`, `/api/economics`, `/api/fees`, `/api/accounts`,
`/api/transactions`, account lookup, account transaction history, and
transaction submission. The `/api/airdrop` endpoint is available through the
wallet's manual Airdrop button so diagnostics do not consume the 24-hour claim.
