/**
 * Test script: test Polymarket order placement DIRECTLY (no proxy) vs VIA PROXY.
 *
 * Usage: node test-poly-order.js
 *
 * This will:
 * 1. Load keys from the user store
 * 2. Connect to Polymarket (Gamma API)
 * 3. Find an active BTC 15-min market
 * 4. Fetch the orderbook
 * 5. Test POST /order DIRECTLY from VPS (no proxy) — see if it gets geoblocked
 * 6. Test POST /order VIA PROXY — baseline comparison
 *
 * The order is a BUY YES at 1¢ (FOK) — will NOT fill (price too low).
 * This tests the API response format / geoblock behavior without risking money.
 */

const axios = require('axios');
const { ethers } = require('ethers');
const { HttpsProxyAgent } = require('https-proxy-agent');
const {
  Chain,
  ClobClient,
  OrderType,
  Side,
  createL2Headers,
  orderToJsonV2,
} = require('@polymarket/clob-client-v2');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const users = require('./src/store/users');
const config = require('./src/config');

const CLOB_URL = config.polymarket.baseUrl; // https://clob.polymarket.com
const GAMMA_BASE = 'https://gamma-api.polymarket.com';

async function main() {
  console.log('=== POLYMARKET ORDER TEST ===\n');

  // 1. Load keys
  const userIds = users.listUsers();
  if (userIds.length === 0) {
    console.error('No users found in store');
    process.exit(1);
  }
  const keys = users.getKeys(userIds[0]);
  console.log(`User: ${userIds[0]}`);
  console.log(`API Key: ${keys.polyApiKey?.slice(0, 8)}...`);
  console.log(`Funder: ${keys.polyFunderAddress || 'none'}`);

  if (!keys.polyPrivateKey || !keys.polyApiKey) {
    console.error('Missing Polymarket keys');
    process.exit(1);
  }

  // 2. Set up wallet
  const wallet = new ethers.Wallet(keys.polyPrivateKey);
  const account = privateKeyToAccount(wallet.privateKey);
  const walletClient = createWalletClient({ account, chain: require('viem/chains').polygon, transport: http() });
  console.log(`Wallet: ${wallet.address}`);

  // Load proxy URL
  const proxyUrl = users.getSystemProxy();
  console.log(`Proxy: ${proxyUrl ? proxyUrl.replace(/:[^:@]+@/, ':***@') : 'NONE'}\n`);

  // 3. Find an active BTC 15-min market via Gamma
  console.log('=== FINDING ACTIVE MARKET ===');
  const nowSecs = Math.floor(Date.now() / 1000);
  const currentTs = Math.floor(nowSecs / 900) * 900;
  const nextTs = currentTs + 900;
  const slugs = [`btc-updown-15m-${currentTs}`, `btc-updown-15m-${nextTs}`];

  let market = null;
  for (const slug of slugs) {
    try {
      const resp = await axios.get(`${GAMMA_BASE}/events`, { params: { slug }, timeout: 10000 });
      const events = Array.isArray(resp.data) ? resp.data : [resp.data];
      for (const event of events) {
        if (event?.markets?.length > 0) {
          market = event.markets[0];
          console.log(`Found via slug: ${slug}`);
          break;
        }
      }
      if (market) break;
    } catch (e) {
      if (e.response?.status !== 404) {
        console.log(`Slug ${slug}: ${e.response?.status || e.message}`);
      }
    }
  }

  if (!market) {
    console.error('No active BTC 15-min market found');
    process.exit(1);
  }

  const outcomes = market.outcomes ? JSON.parse(market.outcomes) : [];
  const tokenIds = market.clobTokenIds ? JSON.parse(market.clobTokenIds) : [];
  const prices = market.outcomePrices ? JSON.parse(market.outcomePrices) : [];

  const yesIdx = outcomes.indexOf('Up') !== -1 ? outcomes.indexOf('Up') : 0;
  const tokenId = tokenIds[yesIdx];

  console.log(`Question: ${market.question}`);
  console.log(`Token ID (Up/Yes): ${tokenId?.slice(0, 20)}...`);
  console.log(`Current price: ${prices[yesIdx]}`);
  console.log('');

  // 4. Fetch orderbook (direct — reads don't need proxy)
  console.log('=== ORDERBOOK ===');
  try {
    const bookResp = await axios.get(`${CLOB_URL}/book`, {
      params: { token_id: tokenId },
      timeout: 10000,
    });
    const book = bookResp.data;
    const asks = (book.asks || []).slice(0, 3);
    const bids = (book.bids || []).slice(0, 3);
    console.log('Asks (top 3):', JSON.stringify(asks));
    console.log('Bids (top 3):', JSON.stringify(bids));
  } catch (e) {
    console.error('Book fetch failed:', e.response?.status, e.response?.data || e.message);
  }
  console.log('');

  // 5. Build a test order: BUY YES at 1¢ (FOK) — will NOT fill
  const testPrice = 0.01;  // 1¢ — way below market, FOK will reject "not filled"
  const testSize = 1;

  const clobClient = new ClobClient({ host: CLOB_URL, chain: Chain.POLYGON, signer: walletClient });
  const signedOrder = await clobClient.createOrder({
    tokenID: tokenId,
    price: testPrice,
    size: testSize,
    side: Side.BUY,
  });
  const payload = orderToJsonV2(signedOrder, keys.polyApiKey, OrderType.FOK, false, false);

  const path = '/order';
  const bodyStr = JSON.stringify(payload);
  const headers = await createL2Headers(
    walletClient,
    { key: keys.polyApiKey, secret: keys.polyApiSecret, passphrase: keys.polyPassphrase },
    { method: 'POST', requestPath: path, body: bodyStr },
  );

  // ============================
  // TEST 1: DIRECT (no proxy)
  // ============================
  console.log('=== TEST 1: DIRECT POST /order (no proxy) ===');
  console.log(`BUY YES @${testPrice} x${testSize} (FOK — will NOT fill)`);
  try {
    const t0 = Date.now();
    const resp = await axios({
      method: 'POST',
      url: `${CLOB_URL}${path}`,
      data: payload,
      headers,
      timeout: 15000,
      maxRedirects: 0,
      proxy: false,
    });
    const latency = Date.now() - t0;
    console.log(`Latency: ${latency}ms`);
    console.log(`Status: ${resp.status}`);
    console.log('Response:', JSON.stringify(resp.data, null, 2));
    if (resp.data?.error) {
      console.log(`>>> CLOB returned error: ${resp.data.error}`);
      console.log('>>> But HTTP 200 means NO GEOBLOCK — direct POST works!');
    } else {
      console.log('>>> Order accepted! Direct POST works!');
    }
  } catch (e) {
    const status = e.response?.status;
    const data = e.response?.data;
    const location = e.response?.headers?.location;
    console.log(`Status: ${status || 'no-response'}`);
    console.log('Error:', JSON.stringify(data || { message: e.message }, null, 2));
    if (location) console.log(`Location header: ${location}`);
    if (status === 403) {
      console.log('>>> HTTP 403 — GEOBLOCKED! Direct POST does NOT work from this IP.');
    } else if (status === 301 || status === 302 || status === 307 || status === 308) {
      console.log('>>> REDIRECT — likely geoblock redirect.');
    } else {
      console.log(`>>> HTTP ${status} — unexpected. Check response above.`);
    }
  }
  console.log('');

  // ============================
  // TEST 2: VIA PROXY
  // ============================
  if (!proxyUrl) {
    console.log('=== TEST 2: SKIPPED (no proxy configured) ===');
  } else {
    console.log('=== TEST 2: PROXY POST /order ===');
    console.log(`BUY YES @${testPrice} x${testSize} (FOK — will NOT fill)`);

    // Need fresh signature (timestamp changed)
    const signedOrder2 = await clobClient.createOrder({
      tokenID: tokenId,
      price: testPrice,
      size: testSize,
      side: Side.BUY,
    });
    const payload2 = orderToJsonV2(signedOrder2, keys.polyApiKey, OrderType.FOK, false, false);

    const bodyStr2 = JSON.stringify(payload2);
    const headers2 = await createL2Headers(
      walletClient,
      { key: keys.polyApiKey, secret: keys.polyApiSecret, passphrase: keys.polyPassphrase },
      { method: 'POST', requestPath: path, body: bodyStr2 },
    );

    const proxyAgent = new HttpsProxyAgent(proxyUrl, { keepAlive: true });

    try {
      const t0 = Date.now();
      const resp = await axios({
        method: 'POST',
        url: `${CLOB_URL}${path}`,
        data: payload2,
        headers: headers2,
        timeout: 15000,
        maxRedirects: 0,
        proxy: false,
        httpsAgent: proxyAgent,
      });
      const latency = Date.now() - t0;
      console.log(`Latency: ${latency}ms`);
      console.log(`Status: ${resp.status}`);
      console.log('Response:', JSON.stringify(resp.data, null, 2));
    } catch (e) {
      const status = e.response?.status;
      const data = e.response?.data;
      console.log(`Status: ${status || 'no-response'}`);
      console.log('Error:', JSON.stringify(data || { message: e.message }, null, 2));
    }
  }
  console.log('');

  // ============================
  // SUMMARY
  // ============================
  console.log('=== SUMMARY ===');
  console.log('If Test 1 returned HTTP 200 (even with "not filled" error) → proxy is UNNECESSARY');
  console.log('If Test 1 returned HTTP 403 or redirect → proxy is REQUIRED for writes');
  console.log('Compare latencies: Direct vs Proxy to quantify the overhead');

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
