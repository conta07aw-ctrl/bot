/**
 * Test script: derive Builder API keys and inspect the response format.
 * Usage: node scripts/test-builder-keys.js
 */
const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { Chain, ClobClient, SignatureTypeV2 } = require('@polymarket/clob-client-v2');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const config = require('../src/config');
const userStore = require('../src/store/users');

async function main() {
  const users = userStore.listUsers();
  if (users.length === 0) { console.log('No users'); return; }

  const keys = userStore.getKeys(users[0]);
  if (!keys?.polyPrivateKey) { console.log('No poly private key'); return; }

  // Set up proxy
  const proxyUrl = config.polymarket.proxyUrl;
  if (proxyUrl) {
    https.globalAgent = new HttpsProxyAgent(proxyUrl);
    console.log('Proxy set:', proxyUrl.replace(/:([^:@]+)@/, ':***@'));
  }

  // Create viem wallet
  const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
  const account = privateKeyToAccount(pk);
  const viemWallet = createWalletClient({ account, chain: polygon, transport: http() });
  console.log('EOA:', account.address);
  console.log('Funder:', keys.polyFunderAddress || 'none');

  // Create authenticated clobClient (same as polymarket connector does)
  const clobUrl = config.polymarket.baseUrl;
  const signerAddr = account.address.toLowerCase();
  const proxyAddr = (keys.polyFunderAddress || '').toLowerCase();
  const useProxy = proxyAddr && proxyAddr !== signerAddr;
  const sigType = useProxy ? SignatureTypeV2.POLY_PROXY : SignatureTypeV2.EOA;

  // First derive CLOB API keys
  const tempClient = new ClobClient({ host: clobUrl, chain: Chain.POLYGON, signer: viemWallet });
  const creds = await tempClient.createOrDeriveApiKey();
  console.log('\n--- CLOB API keys ---');
  console.log('key:', creds.key);
  console.log('secret:', creds.secret?.slice(0, 20) + '...');
  console.log('passphrase:', creds.passphrase?.slice(0, 20) + '...');

  // Create authenticated client
  const client = new ClobClient({
    host: clobUrl,
    chain: Chain.POLYGON,
    signer: viemWallet,
    creds,
    signatureType: sigType,
    funderAddress: keys.polyFunderAddress || undefined,
  });

  // Get existing builder keys
  console.log('\n--- getBuilderApiKeys() ---');
  try {
    const existing = await client.getBuilderApiKeys();
    console.log('Response:', JSON.stringify(existing, null, 2));
  } catch (err) {
    console.log('Error:', err.message);
  }

  // Create new builder key
  console.log('\n--- createBuilderApiKey() ---');
  try {
    const resp = await client.createBuilderApiKey();
    console.log('Response:', JSON.stringify(resp, null, 2));
    console.log('\nResponse keys:', Object.keys(resp || {}));
  } catch (err) {
    console.log('Error:', err.message);
  }
}

main().catch(console.error);
