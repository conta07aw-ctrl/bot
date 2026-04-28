/**
 * Test Polymarket order signing with different signature types.
 * Tests EOA vs POLY_PROXY to diagnose "invalid signature" errors.
 *
 * Usage: node scripts/test-poly-order.js
 * (Run on VPS with proxy configured)
 */

const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { Chain, ClobClient, OrderType, Side, SignatureTypeV2 } = require('@polymarket/clob-client-v2');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const userStore = require('../src/store/users');

const CLOB_URL = 'https://clob.polymarket.com';
const CHAIN_ID = 137;

async function main() {
  const users = userStore.listUsers();
  if (users.length === 0) { console.error('No users found.'); process.exit(1); }

  const keys = userStore.getKeys(users[0]);
  const proxyUrl = userStore.getSystemProxy();

  console.log(`User: ${users[0]}`);
  console.log(`Proxy: ${proxyUrl ? 'YES' : 'NONE'}`);

  if (!keys.polyPrivateKey) { console.error('No Poly private key.'); process.exit(1); }

  // Set up proxy at Node.js level
  if (proxyUrl) {
    const agent = new HttpsProxyAgent(proxyUrl);
    https.globalAgent = agent;
    console.log('Proxy agent set on https.globalAgent');
  }

  const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
  const account = privateKeyToAccount(pk);
  const walletClient = createWalletClient({ account, chain: polygon, transport: http() });

  console.log(`EOA wallet: ${account.address}`);
  console.log(`Funder address from config: ${keys.polyFunderAddress || 'NOT SET'}`);
  console.log('');

  // Step 1: Derive API keys
  console.log('=== Step 1: Derive API keys ===');
  let creds;
  try {
    const client = new ClobClient({ host: CLOB_URL, chain: Chain.POLYGON, signer: walletClient });
    creds = await client.createOrDeriveApiKey();
    console.log(`Derived: key=${creds.key.slice(0, 12)}...`);
  } catch (err) {
    console.error('Derive FAILED:', err.message);
    process.exit(1);
  }

  // Step 2: Get open orders (to test auth works)
  console.log('\n=== Step 2: Test L2 auth (getApiKeys) ===');
  try {
    const authedClient = new ClobClient({
      host: CLOB_URL,
      chain: Chain.POLYGON,
      signer: walletClient,
      creds: { key: creds.key, secret: creds.secret, passphrase: creds.passphrase },
    });
    const apiKeys = await authedClient.getApiKeys();
    console.log(`getApiKeys OK: ${JSON.stringify(apiKeys).slice(0, 200)}`);
  } catch (err) {
    console.error('getApiKeys FAILED:', err.response?.data || err.message);
  }

  // Step 3: Try createOrder with EOA mode (sigType=0, no funder)
  console.log('\n=== Step 3: Create order — EOA mode (sigType=0, no funder) ===');
  try {
    const eoaClient = new ClobClient({
      host: CLOB_URL,
      chain: Chain.POLYGON,
      signer: walletClient,
      creds: { key: creds.key, secret: creds.secret, passphrase: creds.passphrase },
      signatureType: SignatureTypeV2.EOA,
    });

    // Use a real token ID from an active market — we'll get the first one
    const markets = await eoaClient.getSamplingMarkets();
    // Find any active CLOB market with a token
    let testTokenId = null;
    if (markets && markets.data) {
      for (const m of markets.data) {
        if (m.tokens && m.tokens.length > 0 && m.active) {
          testTokenId = m.tokens[0].token_id;
          console.log(`Using market: ${m.question?.slice(0, 60)}...`);
          console.log(`Token ID: ${testTokenId.slice(0, 20)}...`);
          break;
        }
      }
    }

    if (!testTokenId) {
      console.log('No active market found for test, skipping order test');
    } else {
      // Create order but DON'T post it — just build it to test signing
      const order = await eoaClient.createOrder({
        tokenID: testTokenId,
        price: 0.01, // minimum price, won't match
        size: 1,
        side: 'BUY',
      });
      console.log('EOA createOrder OK (signed):', JSON.stringify(order).slice(0, 200));

      // Now try posting it
      const resp = await eoaClient.postOrder(order);
      console.log('EOA postOrder response:', JSON.stringify(resp).slice(0, 300));
    }
  } catch (err) {
    const data = err.response?.data || err.message;
    console.error('EOA order FAILED:', typeof data === 'object' ? JSON.stringify(data) : data);
  }

  // Step 4: Try with POLY_PROXY mode (sigType=1, with funder)
  if (keys.polyFunderAddress && keys.polyFunderAddress !== account.address) {
    console.log(`\n=== Step 4: Create order — POLY_PROXY mode (sigType=1, funder=${keys.polyFunderAddress.slice(0, 10)}...) ===`);
    try {
      const proxyClient = new ClobClient({
        host: CLOB_URL,
        chain: Chain.POLYGON,
        signer: walletClient,
        creds: { key: creds.key, secret: creds.secret, passphrase: creds.passphrase },
        signatureType: SignatureTypeV2.POLY_PROXY,
        funderAddress: keys.polyFunderAddress,
      });

      const markets = await proxyClient.getSamplingMarkets();
      let testTokenId = null;
      if (markets && markets.data) {
        for (const m of markets.data) {
          if (m.tokens && m.tokens.length > 0 && m.active) {
            testTokenId = m.tokens[0].token_id;
            break;
          }
        }
      }

      if (testTokenId) {
        const order = await proxyClient.createOrder({
          tokenID: testTokenId,
          price: 0.01,
          size: 1,
          side: 'BUY',
        });
        console.log('POLY_PROXY createOrder OK (signed):', JSON.stringify(order).slice(0, 200));

        const resp = await proxyClient.postOrder(order);
        console.log('POLY_PROXY postOrder response:', JSON.stringify(resp).slice(0, 300));
      }
    } catch (err) {
      const data = err.response?.data || err.message;
      console.error('POLY_PROXY order FAILED:', typeof data === 'object' ? JSON.stringify(data) : data);
    }
  } else {
    console.log('\n=== Step 4: SKIPPED — no separate funderAddress configured ===');
  }

  console.log('\nDone.');
}

main().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
