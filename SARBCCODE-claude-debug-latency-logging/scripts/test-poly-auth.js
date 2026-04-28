/**
 * Test Polymarket CLOB authentication using the OFFICIAL @polymarket/clob-client + viem.
 * Usage: node scripts/test-poly-auth.js
 */

require('dotenv').config();
const { Chain, ClobClient } = require('@polymarket/clob-client-v2');
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
  console.log(`Proxy: ${proxyUrl ? 'configured' : 'NONE'}`);

  if (!keys.polyPrivateKey) { console.error('No Poly private key.'); process.exit(1); }

  // Create viem wallet client (what @polymarket/clob-client expects)
  const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
  const account = privateKeyToAccount(pk);
  const walletClient = createWalletClient({
    account,
    chain: polygon,
    transport: http(),
  });

  console.log(`Signer wallet: ${account.address}`);
  console.log(`Funder address: ${keys.polyFunderAddress || 'none'}`);
  console.log('');

  // ---- Step 1: Derive API key using official client ----
  console.log('=== Step 1: Derive API key (official client) ===');

  try {
    const client = new ClobClient({ host: CLOB_URL, chain: Chain.POLYGON, signer: walletClient });

    const derivedCreds = await client.createOrDeriveApiKey();
    console.log('DERIVED API KEY SUCCESS!');
    console.log('Full response:', JSON.stringify(derivedCreds, null, 2));
    console.log(`  apiKey: ${derivedCreds.apiKey || derivedCreds.key || derivedCreds.api_key}`);
    console.log(`  secret: ${(derivedCreds.secret || derivedCreds.apiSecret)?.slice(0, 16)}...`);
    console.log(`  passphrase: ${(derivedCreds.passphrase || derivedCreds.apiPassphrase)?.slice(0, 16)}...`);
    console.log('');

    // ---- Step 2: Test L2 auth with derived keys ----
    console.log('=== Step 2: Test with derived keys ===');

    const creds = {
      key: derivedCreds.apiKey || derivedCreds.key || derivedCreds.api_key,
      secret: derivedCreds.secret || derivedCreds.apiSecret,
      passphrase: derivedCreds.passphrase || derivedCreds.apiPassphrase,
    };
    console.log('Using creds:', { key: creds.key, secret: creds.secret?.slice(0, 12) + '...', passphrase: creds.passphrase?.slice(0, 12) + '...' });
    const authedClient = new ClobClient({ host: CLOB_URL, chain: Chain.POLYGON, signer: walletClient, creds });

    const apiKeys = await authedClient.getApiKeys();
    console.log(`GET /auth/api-keys SUCCESS: ${JSON.stringify(apiKeys).slice(0, 300)}`);
    console.log('');

  } catch (err) {
    const status = err.response?.status || err.status || 'unknown';
    const data = err.response?.data ? JSON.stringify(err.response.data) : err.message;
    console.error(`FAILED (${status}): ${data}`);
    console.log('');
  }

  // ---- Step 3: Test with Builder Code keys ----
  console.log('=== Step 3: Test with BUILDER CODE keys ===');
  console.log(`  API Key: ${keys.polyApiKey}`);

  if (keys.polyApiKey && keys.polyApiSecret && keys.polyPassphrase) {
    try {
      const builderClient = new ClobClient({
        host: CLOB_URL,
        chain: Chain.POLYGON,
        signer: walletClient,
        creds: {
          key: keys.polyApiKey,
          secret: keys.polyApiSecret,
          passphrase: keys.polyPassphrase,
        },
      });

      const apiKeys = await builderClient.getApiKeys();
      console.log(`GET /auth/api-keys SUCCESS: ${JSON.stringify(apiKeys).slice(0, 300)}`);
    } catch (err) {
      const status = err.response?.status || err.status || 'unknown';
      const data = err.response?.data ? JSON.stringify(err.response.data) : err.message;
      console.error(`FAILED (${status}): ${data}`);
    }
  }
}

main().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
