# Sylas Coinflip App

Isolated Rust dapp service for Sylas coinflip.

This service is intentionally separate from the Sylas node. It talks to the
node through HTTP API calls and uses the Sylas Wallet extension provider in the
browser. The web page never asks players for recovery keys or private keys.

## Dokploy

Deploy this as a separate app:

```text
Dockerfile path: apps/coinflip/Dockerfile
Container port: 5080
```

Required env:

```text
SYLAS_COINFLIP_BIND=0.0.0.0:5080
SYLAS_NODE_URL=http://sylas:5000
SYLAS_COINFLIP_HOUSE_SECRET_HEX=32_BYTE_HEX_SEED
```

`SYLAS_NODE_URL` must be reachable from the coinflip container. If this app is
not on the same Docker network as the node, point it at an internal HTTP address
for the node API.

The max bet is fixed at `0.1 SYLAS`. The browser preflights this before asking
the wallet to sign, and the backend attempts to refund invalid over-limit stakes
submitted outside the UI.
