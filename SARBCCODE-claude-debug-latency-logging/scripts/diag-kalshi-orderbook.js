#!/usr/bin/env node
/**
 * Diagnostic: fetch a Kalshi orderbook via REST and dump the FULL raw response.
 *
 * Purpose: see ALL fields in the orderbook response — verify whether Kalshi
 * provides separate bid/ask arrays or just yes_dollars/no_dollars (bids only).
 *
 * Usage (from /SARB on the production droplet):
 *   node scripts/diag-kalshi-orderbook.js
 *
 * Requires: data/users.json with Kalshi keys configured.
 */

const crypto = require('crypto');
const axios = require('axios');
const path = require('path');
const fs = require('fs');

const BASE_URL = 'https://api.elections.kalshi.com';
const API_BASE = '/trade-api/v2';

// ---- Load keys from user store (same logic as the app) ----
const DATA_DIR = path.join(__dirname, '..', 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const SYSTEM_FILE = path.join(DATA_DIR, 'system.json');

function getEncKey() {
  // Load .env for DASHBOARD_SECRET
  require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
  const secret = process.env.DASHBOARD_SECRET || 'sarbccode-default-key-change-me';
  return crypto.scryptSync(secret, 'sarbccode-salt', 32);
}

function decrypt(data) {
  if (!data || !data.includes(':')) return '';
  const [ivHex, tagHex, enc] = data.split(':');
  const key = getEncKey();
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(ivHex, 'hex'));
  decipher.setAuthTag(Buffer.from(tagHex, 'hex'));
  let dec = decipher.update(enc, 'hex', 'utf8');
  dec += decipher.final('utf8');
  return dec;
}

function loadKalshiKeys() {
  if (!fs.existsSync(USERS_FILE)) {
    console.error('No users.json found at', USERS_FILE);
    process.exit(1);
  }
  const users = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
  const username = Object.keys(users)[0];
  if (!username) {
    console.error('No users found');
    process.exit(1);
  }
  const user = users[username];
  const apiKey = decrypt(user.keys?.kalshiApiKey);
  const privateKeyPem = decrypt(user.keys?.kalshiPrivateKeyPem);
  if (!apiKey || !privateKeyPem) {
    console.error('No Kalshi keys found for user:', username);
    process.exit(1);
  }
  console.log(`Loaded Kalshi keys for user: ${username}`);
  return { apiKey, privateKeyPem };
}

// ---- Kalshi auth ----
function sign(privateKeyPem, timestampMs, method, apiPath) {
  const pathWithoutQuery = apiPath.split('?')[0];
  const message = `${timestampMs}${method}${pathWithoutQuery}`;
  const signature = crypto.sign('sha256', Buffer.from(message), {
    key: privateKeyPem,
    padding: crypto.constants.RSA_PKCS1_PSS_PADDING,
    saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST,
  });
  return signature.toString('base64');
}

function authHeaders(apiKey, privateKeyPem, method, apiPath) {
  const ts = Date.now().toString();
  return {
    'KALSHI-ACCESS-KEY': apiKey,
    'KALSHI-ACCESS-SIGNATURE': sign(privateKeyPem, ts, method, apiPath),
    'KALSHI-ACCESS-TIMESTAMP': ts,
    'Content-Type': 'application/json',
  };
}

async function get(apiKey, privateKeyPem, apiPath) {
  const url = `${BASE_URL}${apiPath}`;
  const resp = await axios.get(url, {
    headers: authHeaders(apiKey, privateKeyPem, 'GET', apiPath),
    timeout: 10000,
  });
  return resp.data;
}

// ---- Main ----
async function main() {
  const { apiKey, privateKeyPem } = loadKalshiKeys();

  // Step 1: Find an active 15-min market (same logic as kalshi.js getMarkets)
  console.log('\n=== Step 1: Discovering active markets ===');
  const SERIES = ['KXBTC15M', 'KXETH15M'];
  let market = null;

  for (const series of SERIES) {
    console.log(`Trying series: ${series}...`);
    try {
      const data = await get(apiKey, privateKeyPem,
        `${API_BASE}/markets?status=open&series_ticker=${series}&limit=50`
      );
      const markets = data.markets || [];
      if (markets.length === 0) {
        console.log(`  No markets found for ${series}`);
        continue;
      }

      // Find nearest future expiry (same as connector)
      const now = new Date();
      const future = markets.filter(m => {
        const exp = new Date(m.expiration_time || m.close_time || '');
        return exp > now;
      });

      if (future.length === 0) {
        console.log(`  ${markets.length} markets but all expired`);
        continue;
      }

      // Pick one with volume if possible
      const withVol = future.filter(m => (m.volume || 0) > 0);
      market = (withVol.length > 0 ? withVol[0] : future[0]);

      // Also dump ALL fields from the /markets response for one market
      console.log(`\n  === /markets response keys for one market ===`);
      console.log(`  Keys: ${Object.keys(market).join(', ')}`);

      console.log(`  Found ${future.length} active markets in ${series}`);
      break;
    } catch (err) {
      console.log(`  Error: ${err.response?.data?.message || err.message}`);
    }
  }

  if (!market) {
    console.error('No active markets found in any series');
    process.exit(1);
  }
  const ticker = market.ticker;
  console.log(`Using ticker: ${ticker} (${market.title || market.subtitle})`);
  console.log(`Market yes_ask: ${market.yes_ask_dollars || market.yes_ask}, yes_bid: ${market.yes_bid_dollars || market.yes_bid}`);
  console.log(`Market no_ask: ${market.no_ask_dollars || market.no_ask}, no_bid: ${market.no_bid_dollars || market.no_bid}`);

  // Step 2: Fetch the orderbook — dump FULL raw response
  console.log('\n=== Step 2: Raw orderbook response ===');
  const orderbookRaw = await get(apiKey, privateKeyPem,
    `${API_BASE}/markets/${ticker}/orderbook`
  );

  // Print ALL top-level keys
  console.log('\nTop-level keys:', Object.keys(orderbookRaw));

  // Print all nested keys recursively
  function dumpKeys(obj, prefix = '') {
    for (const [key, val] of Object.entries(obj)) {
      const fullKey = prefix ? `${prefix}.${key}` : key;
      if (val && typeof val === 'object' && !Array.isArray(val)) {
        console.log(`  ${fullKey}: {object}`);
        dumpKeys(val, fullKey);
      } else if (Array.isArray(val)) {
        console.log(`  ${fullKey}: [array, ${val.length} items]`);
        if (val.length > 0) {
          console.log(`    first item: ${JSON.stringify(val[0])}`);
          if (val.length > 1) console.log(`    second item: ${JSON.stringify(val[1])}`);
        }
      } else {
        console.log(`  ${fullKey}: ${JSON.stringify(val)}`);
      }
    }
  }
  dumpKeys(orderbookRaw);

  // Also dump the complete JSON (truncated for readability)
  const fullJson = JSON.stringify(orderbookRaw, null, 2);
  if (fullJson.length < 5000) {
    console.log('\n=== Full JSON ===');
    console.log(fullJson);
  } else {
    console.log('\n=== Full JSON (first 5000 chars) ===');
    console.log(fullJson.slice(0, 5000));
  }

  // Step 3: Analysis
  console.log('\n=== Step 3: Analysis ===');
  const fp = orderbookRaw.orderbook_fp || orderbookRaw.orderbook || orderbookRaw;
  const allKeys = Object.keys(fp);
  console.log('Orderbook keys:', allKeys);

  // Check if there are bid/ask specific keys we're missing
  const bidAskKeys = allKeys.filter(k =>
    k.includes('bid') || k.includes('ask') || k.includes('sell') || k.includes('buy')
  );
  if (bidAskKeys.length > 0) {
    console.log('\n*** FOUND BID/ASK SPECIFIC KEYS:', bidAskKeys, '***');
    for (const k of bidAskKeys) {
      const val = fp[k];
      if (Array.isArray(val)) {
        console.log(`  ${k}: ${val.length} levels, first: ${JSON.stringify(val[0])}`);
      } else {
        console.log(`  ${k}:`, val);
      }
    }
  } else {
    console.log('No bid/ask specific keys found — only:', allKeys);
  }

  // Parse yes/no and check sums
  const yesLevels = (fp.yes_dollars || fp.yes || []).map(e => parseFloat(e[0])).filter(p => p > 0);
  const noLevels = (fp.no_dollars || fp.no || []).map(e => parseFloat(e[0])).filter(p => p > 0);

  if (yesLevels.length > 0 && noLevels.length > 0) {
    const bestYes = Math.max(...yesLevels);
    const bestNo = Math.max(...noLevels);
    const worstYes = Math.min(...yesLevels);
    const worstNo = Math.min(...noLevels);

    console.log(`\nYES levels: ${yesLevels.length} | range: $${worstYes} - $${bestYes}`);
    console.log(`NO  levels: ${noLevels.length} | range: $${worstNo} - $${bestNo}`);
    console.log(`Sum of highest: $${(bestYes + bestNo).toFixed(4)} (if BIDS: should be < 1.00)`);
    console.log(`Sum of lowest:  $${(worstYes + worstNo).toFixed(4)} (if ASKS: should be > 1.00)`);

    // Compare with market quotes
    const mktYesAsk = parseFloat(market.yes_ask_dollars || market.yes_ask || 0);
    const mktYesBid = parseFloat(market.yes_bid_dollars || market.yes_bid || 0);
    const mktNoAsk = parseFloat(market.no_ask_dollars || market.no_ask || 0);
    const mktNoBid = parseFloat(market.no_bid_dollars || market.no_bid || 0);

    const derivedYesAsk = Math.round((1 - bestNo) * 100) / 100;
    const derivedNoAsk = Math.round((1 - bestYes) * 100) / 100;

    console.log(`\nMarket quotes (from /markets):  yesAsk=$${mktYesAsk} yesBid=$${mktYesBid} noAsk=$${mktNoAsk} noBid=$${mktNoBid}`);
    console.log(`Derived from book (1-oppBid): yesAsk=$${derivedYesAsk} noAsk=$${derivedNoAsk}`);
    console.log(`Match? yesAsk: ${derivedYesAsk === mktYesAsk ? 'YES ✓' : `NO ✗ (diff: ${(derivedYesAsk - mktYesAsk).toFixed(4)})`}`);
    console.log(`Match? noAsk:  ${derivedNoAsk === mktNoAsk ? 'YES ✓' : `NO ✗ (diff: ${(derivedNoAsk - mktNoAsk).toFixed(4)})`}`);
  }
}

main().catch(err => {
  console.error('Error:', err.response?.data || err.message);
  process.exit(1);
});
