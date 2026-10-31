# Sylas Wallet Extension

Chrome/Edge unpacked extension for Sylas.

## Load

1. Open `chrome://extensions`.
2. Enable Developer mode.
3. Click **Load unpacked**.
4. Select this `extension` folder.

## Network

The extension is hardwired to the hosted Sylas node:

```text
https://web3.sylas.lol
```

Users do not need to configure a node URL.

## Features

- First-run create/import wallet flow.
- Recovery-key backup screen before entering the wallet.
- Store keys in browser extension storage.
- Inject a `window.sylas` provider for approved Sylas dapps.
- View account balance and nonce.
- Receive with public address and QR code.
- Request a manual `1 SYLAS` airdrop once every 24 hours.
- Quote the current variable fee from node congestion.
- Sign and submit transfers to `POST /api/transactions`.
- Let dapps request signed transfers without exposing private keys to the page.
- Test node HTTP endpoints from Network Diagnostics without spending the daily airdrop.
- Test recent transaction and wallet transaction-history endpoints.
- Submit raw transaction lines.

## Token Cap

The wallet displays and enforces the Sylas cap as:

```text
1,000,000 SYLAS
```

The wallet and node use 8 decimal places. The node enforces the same cap for
genesis allocation and airdrop minting.
