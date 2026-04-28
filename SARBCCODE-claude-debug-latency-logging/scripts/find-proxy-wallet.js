/**
 * Find the Polymarket proxy wallet address for a given EOA.
 * Queries the Polymarket Proxy Factory contract on Polygon.
 *
 * Usage: node scripts/find-proxy-wallet.js
 */

const { createPublicClient, http } = require('viem');
const { polygon } = require('viem/chains');
const userStore = require('../src/store/users');
const { privateKeyToAccount } = require('viem/accounts');

// Polymarket Proxy Wallet Factory on Polygon
// This is the known factory address that deploys proxy wallets for users
const PROXY_FACTORY = '0xaB45c5A4B0c941a2F231C04C3f49182e1A254052';

// ABI for getProxy(address) -> address
const FACTORY_ABI = [
  {
    inputs: [{ name: '_addr', type: 'address' }],
    name: 'getProxy',
    outputs: [{ name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
];

async function main() {
  const users = userStore.listUsers();
  if (users.length === 0) { console.error('No users.'); process.exit(1); }

  const keys = userStore.getKeys(users[0]);
  if (!keys.polyPrivateKey) { console.error('No private key.'); process.exit(1); }

  const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
  const account = privateKeyToAccount(pk);

  console.log(`EOA: ${account.address}`);
  console.log(`Currently configured funder: ${keys.polyFunderAddress || 'NOT SET'}`);
  console.log('');

  // Try multiple RPC endpoints
  const rpcs = [
    'https://polygon-rpc.com',
    'https://rpc.ankr.com/polygon',
    'https://polygon.drpc.org',
    'https://1rpc.io/matic',
  ];

  for (const rpc of rpcs) {
    try {
      console.log(`Trying RPC: ${rpc}`);
      const client = createPublicClient({
        chain: polygon,
        transport: http(rpc),
      });

      const proxyAddress = await client.readContract({
        address: PROXY_FACTORY,
        abi: FACTORY_ABI,
        functionName: 'getProxy',
        args: [account.address],
      });

      console.log(`\n✓ PROXY WALLET FOUND: ${proxyAddress}`);

      if (proxyAddress === '0x0000000000000000000000000000000000000000') {
        console.log('\n⚠ Proxy wallet is zero address — no proxy deployed for this EOA.');
        console.log('You should use EOA mode (sigType=0) without funderAddress.');
      } else if (keys.polyFunderAddress && keys.polyFunderAddress.toLowerCase() === proxyAddress.toLowerCase()) {
        console.log('\n✓ Your configured funderAddress matches! The issue is elsewhere.');
      } else {
        console.log(`\n⚠ MISMATCH! Your config has: ${keys.polyFunderAddress}`);
        console.log(`   Correct proxy wallet:    ${proxyAddress}`);
        console.log('\nUpdate POLY_FUNDER_ADDRESS in the dashboard to the correct address above.');
      }
      return;
    } catch (err) {
      console.log(`  Failed: ${err.message.slice(0, 80)}`);
    }
  }

  console.error('\nAll RPCs failed. Check internet connectivity.');
}

main().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
