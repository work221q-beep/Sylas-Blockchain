const CHAIN_ID = "sylas-mainnet";
const TOKEN_SYMBOL = "SYLAS";
const TOKEN_DECIMALS = 8;
const MAX_SUPPLY_NATIVE = 1_000_000;
const MAX_SUPPLY_BASE_UNITS = 100_000_000_000_000n;
const HOSTED_NODE_URL = "https://web3.sylas.lol";

const $ = (id) => document.getElementById(id);
const encoder = new TextEncoder();
const historyStack = [];

let wallet = null;
let latestStatus = null;
let latestReport = {};
let pendingWallet = null;
let reviewingSecret = false;

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
}

function storageGet(keys) {
  return new Promise((resolve) => chrome.storage.local.get(keys, resolve));
}

function storageSet(values) {
  return new Promise((resolve) => chrome.storage.local.set(values, resolve));
}

function storageRemove(keys) {
  return new Promise((resolve) => chrome.storage.local.remove(keys, resolve));
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

function b64(bytes) {
  return btoa(String.fromCharCode(...new Uint8Array(bytes)));
}

function b64Text(text) {
  return btoa(String.fromCharCode(...encoder.encode(text)));
}

function textFromB64(value) {
  return new TextDecoder().decode(Uint8Array.from(atob(value), (char) => char.charCodeAt(0)));
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

function baseUnits(value, decimals = TOKEN_DECIMALS) {
  const raw = String(value || "0").trim();
  if (!/^\d+(\.\d+)?$/.test(raw)) throw new Error("bad amount");
  const [whole, frac = ""] = raw.split(".");
  if (frac.length > decimals) throw new Error("too many decimal places");
  return BigInt(whole || "0") * (10n ** BigInt(decimals))
    + BigInt((frac + "0".repeat(decimals)).slice(0, decimals) || "0");
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

function sylasUri(address) {
  return `sylas:${address}`;
}

function exportSecret(targetWallet) {
  return `sylas:${b64Text(JSON.stringify({
    version: 1,
    chain: CHAIN_ID,
    publicKey: targetWallet.publicKey,
    privateKey: targetWallet.privateKey,
  }))}`;
}

function parseSecret(input) {
  const raw = String(input || "").trim();
  if (!raw) throw new Error("recovery key is required");

  let payload;
  if (raw.startsWith("sylas:")) {
    payload = JSON.parse(textFromB64(raw.slice("sylas:".length)));
  } else if (raw.startsWith("{")) {
    payload = JSON.parse(raw);
  } else {
    throw new Error("paste a Sylas recovery key export");
  }

  if (payload.chain && payload.chain !== CHAIN_ID) throw new Error("recovery key is for a different chain");
  if (!payload.publicKey || payload.publicKey.length !== 64) throw new Error("recovery key export is missing public key");
  fromHex(payload.publicKey);
  if (!payload.privateKey) throw new Error("recovery key export is missing signing key");
  fromB64(payload.privateKey);
  return { publicKey: payload.publicKey.toLowerCase(), privateKey: payload.privateKey };
}

function setNetworkState(kind, text) {
  $("networkState").className = `pill ${kind}`;
  $("networkState").textContent = text;
}

function showScreen(name, push = true) {
  const current = document.querySelector(".screen.active")?.id?.replace("screen-", "");
  if (push && current && current !== name) historyStack.push(current);
  document.querySelectorAll(".screen").forEach((screen) => {
    screen.classList.toggle("active", screen.id === `screen-${name}`);
  });
  const titles = {
    onboarding: "Welcome",
    import: "Import Wallet",
    backup: "Recovery Key",
    home: "Wallet",
    send: "Send",
    receive: "Receive",
    network: "Network Check",
  };
  $("screenTitle").textContent = titles[name] || "Sylas";
  $("backButton").classList.toggle("hidden", name === "onboarding" || name === "home");
}

function goBack() {
  const previous = historyStack.pop() || (wallet ? "home" : "onboarding");
  showScreen(previous, false);
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

async function createWallet() {
  try {
    const pair = await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
    pendingWallet = {
      publicKey: hex(await crypto.subtle.exportKey("raw", pair.publicKey)),
      privateKey: b64(await crypto.subtle.exportKey("pkcs8", pair.privateKey)),
    };
    reviewingSecret = false;
    $("backupSecret").value = exportSecret(pendingWallet);
    $("backupConfirmed").checked = false;
    $("enterWallet").textContent = "Enter Wallet";
    $("backupStatus").textContent = "";
    showScreen("backup");
  } catch (err) {
    setNetworkState("fail", "Key Error");
    $("importStatus").textContent = `Wallet creation failed: ${err}`;
  }
}

async function finishImport() {
  try {
    wallet = parseSecret($("importSecret").value);
    await storageSet({ wallet });
    $("importSecret").value = "";
    showScreen("home", false);
    await refreshWallet();
  } catch (err) {
    $("importStatus").textContent = `Import failed: ${err}`;
  }
}

async function enterWallet() {
  if (reviewingSecret) {
    reviewingSecret = false;
    $("enterWallet").textContent = "Enter Wallet";
    showScreen("home", false);
    return;
  }
  if (!pendingWallet) return;
  if (!$("backupConfirmed").checked) {
    $("backupStatus").textContent = "Confirm that you saved the recovery key first.";
    return;
  }
  wallet = pendingWallet;
  pendingWallet = null;
  reviewingSecret = false;
  await storageSet({ wallet });
  showScreen("home", false);
  await refreshWallet();
}

async function forgetWallet() {
  await storageRemove(["wallet"]);
  wallet = null;
  pendingWallet = null;
  reviewingSecret = false;
  $("balance").textContent = `0 ${TOKEN_SYMBOL}`;
  $("addressPreview").textContent = "-";
  showScreen("onboarding", false);
}

async function refreshWallet() {
  if (!wallet) {
    showScreen("onboarding", false);
    return;
  }
  $("addressPreview").textContent = shortAddress(wallet.publicKey);
  $("receiveAddress").value = wallet.publicKey;
  $("receiveUri").value = sylasUri(wallet.publicKey);
  $("receiveQr").src = `https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=${encodeURIComponent(sylasUri(wallet.publicKey))}`;
  try {
    latestStatus = await api("/api/status");
    const decimals = latestStatus.economics?.token_decimals ?? TOKEN_DECIMALS;
    const symbol = latestStatus.economics?.token_symbol || TOKEN_SYMBOL;
    const account = await api(`/api/account/${wallet.publicKey}`);
    $("balance").textContent = `${nativeUnits(account.balance, decimals)} ${symbol}`;
    $("nonce").textContent = account.nonce;
    $("homeStatus").textContent = `Connected to Sylas. Supply cap ${MAX_SUPPLY_NATIVE.toLocaleString()} ${TOKEN_SYMBOL}.`;
    setNetworkState("online", "Online");
  } catch (err) {
    $("homeStatus").textContent = `Hosted node unavailable: ${err}`;
    setNetworkState("fail", "Offline");
  }
}

async function copySecret() {
  await navigator.clipboard.writeText($("backupSecret").value);
    $("backupStatus").textContent = "Recovery key copied.";
}

async function copyAddress() {
  await navigator.clipboard.writeText(wallet?.publicKey || "");
}

async function requestAirdrop() {
  try {
    if (!wallet) throw new Error("wallet not loaded");
    const result = await api("/api/airdrop", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: wallet.publicKey,
    });
    $("homeStatus").textContent = `Airdropped ${result.amount_native} ${TOKEN_SYMBOL}. Next request after ${new Date(Number(result.next_available_ms)).toLocaleString()}.`;
    await refreshWallet();
  } catch (err) {
    $("homeStatus").textContent = `Airdrop unavailable: ${err}`;
  }
}

async function refreshFeeQuote() {
  try {
    const quote = await api("/api/fees");
    $("fee").value = quote.recommended_fee;
    $("feeHint").textContent = `Recommended fee ${quote.recommended_fee} base units. Max fee ${quote.max_fee}.`;
  } catch (err) {
    $("feeHint").textContent = `Fee quote unavailable, using default. ${err}`;
  }
}

async function openBackup() {
  if (!wallet) return;
  pendingWallet = null;
  reviewingSecret = true;
  $("backupSecret").value = exportSecret(wallet);
  $("backupConfirmed").checked = true;
  $("enterWallet").textContent = "Done";
  $("backupStatus").textContent = "";
  showScreen("backup");
}

async function sendTransaction() {
  try {
    if (!wallet) throw new Error("wallet not loaded");
    const recipient = $("recipient").value.trim().toLowerCase();
    if (recipient.length !== 64) throw new Error("recipient must be 32-byte hex");
    fromHex(recipient);
    const status = await api("/api/status");
    const decimals = status.economics?.token_decimals ?? TOKEN_DECIMALS;
    const amount = baseUnits($("amount").value, decimals);
    const fee = BigInt(($("fee").value || "0").trim());
    if (amount <= 0n) throw new Error("amount must be greater than zero");
    if (amount > MAX_SUPPLY_BASE_UNITS) throw new Error("amount exceeds 1,000,000 SYLAS cap");

    const account = await api(`/api/account/${wallet.publicKey}`);
    const nonce = BigInt(account.nonce) + 1n;
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
    $("rawTx").value = line;
    const result = await api("/api/transactions", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: line,
    });
    $("sendStatus").textContent = `Queued ${result.tx_id}`;
    await refreshWallet();
  } catch (err) {
    $("sendStatus").textContent = `Send failed: ${err}`;
  }
}

async function submitRaw() {
  try {
    const result = await api("/api/transactions", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: $("rawTx").value.trim(),
    });
    $("endpointJson").textContent = JSON.stringify(result, null, 2);
  } catch (err) {
    $("endpointJson").textContent = JSON.stringify({ error: String(err) }, null, 2);
  }
}

function renderEndpointRows(results) {
  latestReport = { generated_at: new Date().toISOString(), network: "Sylas Mainnet", results };
  $("endpointRows").innerHTML = results.map((item) => (
    `<tr><td><span class="badge ${esc(item.status)}">${esc(item.status)}</span></td><td><code>${esc(item.endpoint)}</code></td><td>${esc(item.detail)}</td></tr>`
  )).join("");
  $("endpointJson").textContent = JSON.stringify(latestReport, null, 2);
}

async function runEndpointTest(test) {
  if (test.requiresWallet && !wallet) {
    return { endpoint: test.label, status: "warn", detail: "wallet required" };
  }
  if (test.requiresRaw && !$("rawTx").value.trim()) {
    return { endpoint: test.label, status: "warn", detail: "raw transaction required" };
  }
  try {
    const result = await test.run();
    return { endpoint: test.label, status: "pass", detail: test.detail(result) };
  } catch (err) {
    return { endpoint: test.label, status: "fail", detail: String(err) };
  }
}

async function testAllEndpoints() {
  const tests = [
    { label: "GET /healthz", run: () => api("/healthz"), detail: (r) => String(r).trim() },
    { label: "GET /api/status", run: () => api("/api/status"), detail: (r) => `height ${r.height}` },
    { label: "GET /api/debug", run: () => api("/api/debug"), detail: (r) => `${r.status} (${r.failures} failures, ${r.warnings} warnings)` },
    { label: "GET /api/consensus", run: () => api("/api/consensus"), detail: (r) => `${r.mode} validators=${r.validator_count}` },
    { label: "GET /api/validators", run: () => api("/api/validators"), detail: (r) => `${r.validators?.length || 0} validators` },
    { label: "GET /api/peers", run: () => api("/api/peers"), detail: (r) => `${r.peers?.length || 0} peers` },
    { label: "GET /api/blocks", run: () => api("/api/blocks"), detail: (r) => `${r.blocks?.length || 0} blocks` },
    { label: "GET /api/economics", run: () => api("/api/economics"), detail: (r) => `supply ${nativeUnits(r.total_supply_base_units, r.token_decimals)}/${r.max_supply_native}` },
    { label: "GET /api/fees", run: () => api("/api/fees"), detail: (r) => `recommended fee ${r.recommended_fee}` },
    { label: "GET /api/accounts", run: () => api("/api/accounts"), detail: (r) => `${r.accounts?.length || 0} accounts` },
    { label: "GET /api/account/<wallet>", requiresWallet: true, run: () => api(`/api/account/${wallet.publicKey}`), detail: (r) => `balance ${r.balance} nonce ${r.nonce}` },
    { label: "GET /api/transactions", run: () => api("/api/transactions"), detail: (r) => `${r.transactions?.length || 0} indexed transactions` },
    { label: "GET /api/account/<wallet>/transactions", requiresWallet: true, run: () => api(`/api/account/${wallet.publicKey}/transactions`), detail: (r) => `${r.transactions?.length || 0} wallet transactions` },
    { label: "POST /api/airdrop", run: async () => ({ manual: true }), detail: () => "manual wallet action only; diagnostics never consume the 24h airdrop" },
    { label: "POST /api/transactions", requiresRaw: true, run: () => api("/api/transactions", { method: "POST", headers: { "content-type": "text/plain" }, body: $("rawTx").value.trim() }), detail: (r) => `queued ${r.tx_id}` },
  ];
  $("endpointRows").innerHTML = '<tr><td colspan="3">Running network checks.</td></tr>';
  const results = [];
  for (const test of tests) results.push(await runEndpointTest(test));
  renderEndpointRows(results);
}

async function copyReport() {
  await navigator.clipboard.writeText(JSON.stringify(latestReport, null, 2));
}

function bindEvents() {
  $("backButton").addEventListener("click", goBack);
  $("startCreate").addEventListener("click", createWallet);
  $("startImport").addEventListener("click", () => showScreen("import"));
  $("finishImport").addEventListener("click", finishImport);
  $("copySecret").addEventListener("click", copySecret);
  $("enterWallet").addEventListener("click", enterWallet);
  $("openSend").addEventListener("click", async () => {
    showScreen("send");
    await refreshFeeQuote();
  });
  $("openReceive").addEventListener("click", () => showScreen("receive"));
  $("requestAirdrop").addEventListener("click", requestAirdrop);
  $("refreshWallet").addEventListener("click", refreshWallet);
  $("openNetwork").addEventListener("click", () => showScreen("network"));
  $("openBackup").addEventListener("click", openBackup);
  $("forgetWallet").addEventListener("click", forgetWallet);
  $("copyAddress").addEventListener("click", copyAddress);
  $("sendTransaction").addEventListener("click", sendTransaction);
  $("runEndpointTests").addEventListener("click", testAllEndpoints);
  $("copyReport").addEventListener("click", copyReport);
  $("submitRaw").addEventListener("click", submitRaw);
}

async function init() {
  $("hostedNode").textContent = "Sylas Mainnet";
  bindEvents();
  const stored = await storageGet(["wallet"]);
  wallet = stored.wallet || null;
  if (wallet) {
    showScreen("home", false);
    await refreshWallet();
  } else {
    showScreen("onboarding", false);
  }
}

init();
