/**
 * Deep diagnostic for Polymarket "invalid signature" with POLY_PROXY.
 * Tests: proxy on-chain status, ethers vs viem signing, negRisk variations.
 *
 * Usage: node scripts/test-poly-signature-debug.js
 */

const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { ethers } = require('ethers');
const { Chain, ClobClient, SignatureTypeV2 } = require('@polymarket/clob-client-v2');
const { createWalletClient, http, createPublicClient } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const userStore = require('../src/store/users');

const CLOB_URL = 'https://clob.polymarket.com';
const CHAIN_ID = 137;

// Proxy factory on Polygon
const PROXY_FACTORY = '0xaB45c5A4B0c941a2F231C04C3f49182e1A254052';
const FACTORY_ABI = [
  { inputs: [{ name: '_addr', type: 'address' }], name: 'getProxy', outputs: [{ name: '', type: 'address' }], stateMutability: 'view', type: 'function' },
];

async function main() {
  const users = userStore.listUsers();
  const keys = userStore.getKeys(users[0]);
  const proxyUrl = userStore.getSystemProxy();

  if (proxyUrl) {
    https.globalAgent = new HttpsProxyAgent(proxyUrl);
    console.log('Proxy: SET');
  }

  const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
  const account = privateKeyToAccount(pk);
  const viemWallet = createWalletClient({ account, chain: polygon, transport: http() });
  const ethersWallet = new ethers.Wallet(pk);

  console.log(`EOA: ${account.address}`);
  console.log(`Ethers address: ${ethersWallet.address}`);
  console.log(`Funder: ${keys.polyFunderAddress || 'NOT SET'}`);
  console.log('');

  // Check 1: Proxy contract on-chain
  console.log('=== Check 1: Proxy contract on-chain ===');
  try {
    const publicClient = createPublicClient({ chain: polygon, transport: http('https://polygon-rpc.com') });
    const proxyAddr = await publicClient.readContract({
      address: PROXY_FACTORY, abi: FACTORY_ABI, functionName: 'getProxy', args: [account.address],
    });
    console.log(`Factory getProxy(${account.address.slice(0, 10)}...) = ${proxyAddr}`);

    const code = await publicClient.getCode({ address: proxyAddr });
    console.log(`Proxy contract code: ${code ? code.slice(0, 20) + '... (' + ((code.length - 2) / 2) + ' bytes)' : 'NO CODE (not deployed!)'}`);

    if (keys.polyFunderAddress && proxyAddr.toLowerCase() !== keys.polyFunderAddress.toLowerCase()) {
      console.log(`!! MISMATCH: configured=${keys.polyFunderAddress}, on-chain=${proxyAddr}`);
    } else {
      console.log('Proxy address matches config.');
    }
  } catch (err) {
    console.error('On-chain check failed:', err.message);
  }

  // Derive API keys
  console.log('\n=== Derive API keys ===');
  let creds;
  try {
    const client = new ClobClient({ host: CLOB_URL, chain: Chain.POLYGON, signer: viemWallet });
    creds = await client.createOrDeriveApiKey();
    console.log(`Derived: key=${creds.key.slice(0, 12)}...`);
  } catch (err) {
    console.error('Derive FAILED:', err.message);
    process.exit(1);
  }

  // Check 2: Try with ethers.js signer instead of viem
  console.log('\n=== Check 2: POLY_PROXY with ethers.js signer ===');
  try {
    const ethersClient = new ClobClient({
      host: CLOB_URL,
      chain: Chain.POLYGON,
      signer: ethersWallet,
      creds: { key: creds.key, secret: creds.secret, passphrase: creds.passphrase },
      signatureType: SignatureTypeV2.POLY_PROXY,
      funderAddress: keys.polyFunderAddress,
    });

    // Find an active market
    const markets = await ethersClient.getSamplingMarkets();
    let testToken = null;
    let testQuestion = '';
    let testNegRisk = false;
    if (markets?.data) {
      for (const m of markets.data) {
        if (m.tokens?.length > 0 && m.active) {
          testToken = m.tokens[0].token_id;
          testQuestion = m.question?.slice(0, 50);
          testNegRisk = m.neg_risk || false;
          break;
        }
      }
    }
    if (!testToken) { console.log('No market found'); return; }

    console.log(`Market: ${testQuestion}...`);
    console.log(`Token: ${testToken.slice(0, 20)}...`);
    console.log(`negRisk: ${testNegRisk}`);

    const order = await ethersClient.createOrder({
      tokenID: testToken, price: 0.01, size: 1, side: 'BUY',
    });
    console.log(`Order created: maker=${order.maker.slice(0, 10)}..., signer=${order.signer.slice(0, 10)}..., sigType=${order.signatureType}`);
    console.log(`Signature: ${order.signature.slice(0, 30)}... (${order.signature.length} chars)`);

    const resp = await ethersClient.postOrder(order);
    console.log(`ethers POLY_PROXY result:`, JSON.stringify(resp).slice(0, 300));
  } catch (err) {
    console.error('ethers POLY_PROXY FAILED:', err.response?.data || err.message);
  }

  // Check 3: Try with viem + POLY_PROXY (what we already know fails)
  console.log('\n=== Check 3: POLY_PROXY with viem signer (confirmation) ===');
  try {
    const viemClient = new ClobClient({
      host: CLOB_URL,
      chain: Chain.POLYGON,
      signer: viemWallet,
      creds: { key: creds.key, secret: creds.secret, passphrase: creds.passphrase },
      signatureType: SignatureTypeV2.POLY_PROXY,
      funderAddress: keys.polyFunderAddress,
    });

    const markets = await viemClient.getSamplingMarkets();
    let testToken = null;
    if (markets?.data) {
      for (const m of markets.data) {
        if (m.tokens?.length > 0 && m.active) { testToken = m.tokens[0].token_id; break; }
      }
    }
    if (!testToken) { console.log('No market'); return; }

    const order = await viemClient.createOrder({
      tokenID: testToken, price: 0.01, size: 1, side: 'BUY',
    });
    console.log(`Order created: maker=${order.maker.slice(0, 10)}..., signer=${order.signer.slice(0, 10)}..., sigType=${order.signatureType}`);
    console.log(`Signature: ${order.signature.slice(0, 30)}... (${order.signature.length} chars)`);

    const resp = await viemClient.postOrder(order);
    console.log(`viem POLY_PROXY result:`, JSON.stringify(resp).slice(0, 300));
  } catch (err) {
    console.error('viem POLY_PROXY FAILED:', err.response?.data || err.message);
  }

  // Check 4: Try EOA with explicitly setting negRisk
  console.log('\n=== Check 4: EOA with negRisk=true vs false ===');
  for (const negRisk of [true, false]) {
    try {
      const client = new ClobClient({
        host: CLOB_URL,
        chain: Chain.POLYGON,
        signer: viemWallet,
        creds: { key: creds.key, secret: creds.secret, passphrase: creds.passphrase },
        signatureType: SignatureTypeV2.EOA,
      });

      const markets = await client.getSamplingMarkets();
      let testToken = null;
      if (markets?.data) {
        for (const m of markets.data) {
          if (m.tokens?.length > 0 && m.active) { testToken = m.tokens[0].token_id; break; }
        }
      }
      if (!testToken) continue;

      const order = await client.createOrder({
        tokenID: testToken, price: 0.01, size: 1, side: 'BUY',
      }, { negRisk });
      console.log(`EOA negRisk=${negRisk}: maker=${order.maker.slice(0, 10)}..., sigType=${order.signatureType}`);

      const resp = await client.postOrder(order);
      console.log(`  Result:`, JSON.stringify(resp).slice(0, 200));
    } catch (err) {
      console.error(`  EOA negRisk=${negRisk} FAILED:`, err.response?.data || err.message);
    }
  }

  console.log('\nDone.');
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
