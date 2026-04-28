/**
 * Test script to debug RTDS subscriptions.
 * Run on VPS: node scripts/test-rtds.js
 *
 * Tests different subscription formats to find which ones
 * return data for ETH (BTC already works).
 */

const WebSocket = require('ws');

const RTDS_URL = 'wss://ws-live-data.polymarket.com';

const ws = new WebSocket(RTDS_URL);
const seen = {};
let msgCount = 0;

ws.on('open', () => {
  console.log('Connected to RTDS');

  // Test 1: Per-symbol subscriptions (current approach — only BTC works)
  const perSymbol = ['btc/usd', 'eth/usd'].map((sym) => ({
    topic: 'crypto_prices_chainlink',
    type: 'update',
    filters: JSON.stringify({ symbol: sym }),
  }));

  // Test 2: Try uppercase symbols
  const uppercase = ['BTC/USD', 'ETH/USD'].map((sym) => ({
    topic: 'crypto_prices_chainlink',
    type: 'update',
    filters: JSON.stringify({ symbol: sym }),
  }));

  // Test 3: Try different symbol formats
  const altFormats = ['ethusd', 'ETHUSD',
                      'eth-usd', 'ETH-USD',
                      'ethereum'].map((sym) => ({
    topic: 'crypto_prices_chainlink',
    type: 'update',
    filters: JSON.stringify({ symbol: sym }),
  }));

  // Test 4: Try different topics
  const altTopics = [
    'crypto_prices', 'chainlink_prices', 'prices',
    'crypto_prices_chainlink_eth',
    'eth_usd',
    'chainlink_eth_usd',
  ].map((topic) => ({
    topic,
    type: 'update',
    filters: '',
  }));

  // Test 5: Try snapshot type
  const snapshots = ['btc/usd', 'eth/usd'].map((sym) => ({
    topic: 'crypto_prices_chainlink',
    type: 'snapshot',
    filters: JSON.stringify({ symbol: sym }),
  }));

  // Test 6: No type, just topic + filters
  const noType = ['eth/usd'].map((sym) => ({
    topic: 'crypto_prices_chainlink',
    filters: JSON.stringify({ symbol: sym }),
  }));

  const allSubs = [...perSymbol, ...uppercase, ...altFormats, ...altTopics, ...snapshots, ...noType];
  console.log(`Subscribing with ${allSubs.length} subscriptions...`);

  ws.send(JSON.stringify({
    action: 'subscribe',
    subscriptions: allSubs,
  }));

  // Ping every 5s
  setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, 5000);
});

ws.on('message', (raw) => {
  msgCount++;
  try {
    const msg = JSON.parse(raw.toString());
    const payload = msg.payload;

    // Track unique topic:type:symbol combos
    let symbols = [];
    if (payload) {
      if (Array.isArray(payload)) {
        symbols = [...new Set(payload.slice(0, 10).map(p => p?.symbol).filter(Boolean))];
      } else if (payload.symbol) {
        symbols = [payload.symbol];
      } else if (payload.data?.symbol) {
        symbols = [payload.data.symbol];
      }
    }

    const key = `${msg.topic}|${msg.type}|${symbols.join(',')}`;
    if (!seen[key]) {
      seen[key] = 0;
      const payloadStr = JSON.stringify(payload).slice(0, 200);
      console.log(`\n[NEW] topic=${msg.topic} type=${msg.type} symbols=[${symbols.join(',')}]`);
      console.log(`  payload: ${payloadStr}`);
    }
    seen[key]++;

    // Log summary every 30 messages
    if (msgCount % 30 === 0) {
      console.log(`\n--- ${msgCount} messages so far ---`);
      for (const [k, v] of Object.entries(seen)) {
        console.log(`  ${k}: ${v} msgs`);
      }
    }
  } catch (e) {
    console.log('Parse error:', e.message);
  }
});

ws.on('close', (code) => console.log('Closed:', code));
ws.on('error', (err) => console.error('Error:', err.message));

// Stop after 90 seconds
setTimeout(() => {
  console.log('\n=== FINAL SUMMARY ===');
  console.log(`Total messages: ${msgCount}`);
  for (const [k, v] of Object.entries(seen)) {
    console.log(`  ${k}: ${v} msgs`);
  }
  ws.close();
  process.exit(0);
}, 90_000);

console.log('Waiting 90 seconds for data...');
