const SYLAS_PROVIDER_EVENT = "SYLAS_PROVIDER_REQUEST";
const SYLAS_PROVIDER_RESPONSE = "SYLAS_PROVIDER_RESPONSE";
const CHAIN_ID = "sylas-mainnet";
const TOKEN_DECIMALS = 8;
const MAX_SUPPLY_BASE_UNITS = 100_000_000_000_000n;
const HOSTED_NODE_URL = "https://web3.sylas.lol";

const encoder = new TextEncoder();

function injectProvider() {
  const script = document.createElement("script");
  script.textContent = `
    (() => {
      if (window.sylas) return;
      let sequence = 1;
      const pending = new Map();
      window.addEventListener("message", (event) => {
        if (event.source !== window || event.data?.type !== "${SYLAS_PROVIDER_RESPONSE}") return;
        const item = pending.get(event.data.id);
        if (!item) return;
        pending.delete(event.data.id);
        if (event.data.error) item.reject(new Error(event.data.error));
        else item.resolve(event.data.result);
      });
      function request(method, params) {
        const id = sequence++;
        return new Promise((resolve, reject) => {
          pending.set(id, { resolve, reject });
          window.postMessage({ type: "${SYLAS_PROVIDER_EVENT}", id, method, params }, window.location.origin);
          setTimeout(() => {
            if (!pending.has(id)) return;
            pending.delete(id);
            reject(new Error("Sylas Wallet did not respond"));
          }, 60000);
        });
      }
      window.sylas = Object.freeze({
        isSylas: true,
        network: "Sylas Mainnet",
        connect: () => request("connect", {}),
        getAccount: () => request("getAccount", {}),
        signAndSendTransfer: (params) => request("signAndSendTransfer", params || {}),
      });
    })();
  `;
  (document.documentElement || document.head).appendChild(script);
  script.remove();
}

function storageGet(keys) {
  return new Promise((resolve) => chrome.storage.local.get(keys, resolve));
}

async function walletRequired() {
  const stored = await storageGet(["wallet"]);
  if (!stored.wallet?.publicKey || !stored.wallet?.privateKey) {
    throw new Error("unlock or create a Sylas Wallet first");
  }
  return stored.wallet;
}

async function api(path, options = {}) {
  const response = await fetch(`${HOSTED_NODE_URL}${path}`, { cache: "no-store", ...options });
  const contentType = response.headers.get("content-type") || "";
  const body = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    throw new Error(typeof body === "string" ? body : (body.error || JSON.stringify(body)));
  }
  return body;
}

function hex(bytes) {
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function fromHex(input) {
  const value = String(input || "").trim().toLowerCase();
  if (value.length % 2 || !/^[0-9a-f]*$/.test(value)) throw new Error("bad hex");
  const out = new Uint8Array(value.length / 2);
  for (let i = 0; i < out.length; i += 1) out[i] = parseInt(value.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function fromB64(value) {
  return Uint8Array.from(atob(String(value || "").trim()), (char) => char.charCodeAt(0));
}

function concat(...parts) {
  const len = parts.reduce((sum, part) => sum + part.length, 0);
  const out = new Uint8Array(len);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function u32(value) {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, Number(value), true);
  return out;
}

function u64(value) {
  const number = BigInt(value);
  if (number < 0n || number > 18_446_744_073_709_551_615n) throw new Error("u64 out of range");
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, number, true);
  return out;
}

function nativeUnits(base, decimals = TOKEN_DECIMALS) {
  const value = BigInt(base || "0");
  const scale = 10n ** BigInt(decimals);
  const whole = value / scale;
  const frac = (value % scale).toString().padStart(decimals, "0").replace(/0+$/, "");
  return frac ? `${whole}.${frac}` : String(whole);
}

function shortAddress(value) {
  return value ? `${value.slice(0, 8)}...${value.slice(-8)}` : "-";
}

function signingPayload({ signer, recipient, nonce, fee, amount, recentHash }) {
  const signerBytes = fromHex(signer);
  const recipientBytes = fromHex(recipient);
  const chain = encoder.encode(CHAIN_ID);
  const recent = fromHex(recentHash);
  const writeSet = signer === recipient ? [signerBytes] : [signerBytes, recipientBytes];
  return concat(
    encoder.encode("sylas-tx-v1"),
    u32(chain.length),
    chain,
    signerBytes,
    u64(nonce),
    u64(fee),
    recent,
    u32(0),
    u32(writeSet.length),
    ...writeSet,
    u32(1),
    signerBytes,
    recipientBytes,
    u64(amount),
  );
}

async function signAndSendTransfer(params) {
  const wallet = await walletRequired();
  const recipient = String(params.to || "").trim().toLowerCase();
  if (recipient.length !== 64) throw new Error("recipient must be a 32-byte hex public key");
  fromHex(recipient);
  const amount = BigInt(String(params.amountBaseUnits || "0"));
  if (amount <= 0n) throw new Error("amount must be greater than zero");
  if (amount > MAX_SUPPLY_BASE_UNITS) throw new Error("amount exceeds Sylas supply cap");

  const label = params.label || "Sylas transfer";
  const ok = confirm(`${label}\n\nSend ${nativeUnits(amount)} SYLAS to ${shortAddress(recipient)}?`);
  if (!ok) throw new Error("user rejected transaction");

  const status = await api("/api/status");
  const fees = await api("/api/fees");
  const account = await api(`/api/account/${wallet.publicKey}`);
  const nonce = BigInt(account.nonce) + 1n;
  const fee = BigInt(fees.recommended_fee || 5000);
  const payload = signingPayload({
    signer: wallet.publicKey,
    recipient,
    nonce,
    fee,
    amount,
    recentHash: status.recent_block_hash,
  });
  const privateKey = await crypto.subtle.importKey(
    "pkcs8",
    fromB64(wallet.privateKey),
    { name: "Ed25519" },
    false,
    ["sign"],
  );
  const signature = hex(await crypto.subtle.sign({ name: "Ed25519" }, privateKey, payload));
  const writeSet = wallet.publicKey === recipient ? wallet.publicKey : `${wallet.publicKey},${recipient}`;
  const line = `TX ${wallet.publicKey} ${signature} ${nonce} ${fee} ${status.recent_block_hash} - ${writeSet} ${wallet.publicKey}:${recipient}:${amount}`;
  const result = await api("/api/transactions", {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: line,
  });
  return { tx_id: result.tx_id, signer: wallet.publicKey, recipient, amount_base_units: amount.toString() };
}

async function handleRequest(message) {
  if (message.method === "connect") {
    const wallet = await walletRequired();
    const ok = confirm(`Connect Sylas Wallet to ${location.host}?`);
    if (!ok) throw new Error("user rejected wallet connection");
    return { publicKey: wallet.publicKey, network: "Sylas Mainnet", chainId: CHAIN_ID };
  }
  if (message.method === "getAccount") {
    const wallet = await walletRequired();
    return api(`/api/account/${wallet.publicKey}`);
  }
  if (message.method === "signAndSendTransfer") {
    return signAndSendTransfer(message.params || {});
  }
  throw new Error("unsupported Sylas provider method");
}

window.addEventListener("message", async (event) => {
  if (event.source !== window || event.data?.type !== SYLAS_PROVIDER_EVENT) return;
  const { id } = event.data;
  try {
    const result = await handleRequest(event.data);
    window.postMessage({ type: SYLAS_PROVIDER_RESPONSE, id, result }, window.location.origin);
  } catch (err) {
    window.postMessage({ type: SYLAS_PROVIDER_RESPONSE, id, error: String(err.message || err) }, window.location.origin);
  }
});

injectProvider();
