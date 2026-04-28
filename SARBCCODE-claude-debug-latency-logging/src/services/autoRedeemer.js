/**
 * Auto-Redeemer — 24/7 background service that automatically redeems
 * winning Polymarket positions for ALL registered users.
 *
 * Flow:
 *   1. Every CHECK_INTERVAL, iterate over all users with Polymarket keys
 *   2. For each user: query positions via public data-api (no auth needed)
 *   3. Filter for won positions (token price >= 97%)
 *   4. Redeem via Polymarket Relayer (gasless, handles proxy wallets)
 *   5. USDC goes back to the user's proxy wallet automatically
 *
 * Position discovery uses the public Polymarket data-api:
 *   https://data-api.polymarket.com/positions?user=WALLET&sizeThreshold=.01
 *
 * Redemption uses the Polymarket Relayer v2:
 *   RelayClient → signs Safe tx → relayer submits on-chain → CTF.redeemPositions
 *   Gasless: the relayer pays gas, user just signs.
 *
 * Builder credentials (for relayer auth) are SEPARATE from CLOB API keys:
 *   - CLOB keys: derived via clobClient.deriveApiKey() — for order placement
 *   - Builder keys: stored in user store OR derived via createBuilderApiKey() — for relayer
 *
 * Polygon contracts:
 *   - ConditionalTokens (CTF):  0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
 *   - PUSD (Polygon):           0x455e53CBB86018Ac2B8092FdCd39c5b8118FA047
 */

const axios = require('axios');
const { createWalletClient, http, encodeFunctionData } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const userStore = require('../store/users');

// 5 minutes — stable and confirmed working
const CHECK_INTERVAL_MS = 300_000;

const DATA_API_BASE = 'https://data-api.polymarket.com';
const RELAYER_URL = 'https://relayer-v2.polymarket.com/';

// ConditionalTokens contract — core redemption
const CTF_CONTRACT = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045';
// PUSD on Polygon (Polymarket USD — replaced USDC.e)
const PUSD_ADDRESS = '0x455e53CBB86018Ac2B8092FdCd39c5b8118FA047';
const ZERO_HASH = '0x' + '0'.repeat(64);

// CTF ABI for encoding redeemPositions calldata
const CTF_REDEEM_ABI = [{
  name: 'redeemPositions',
  type: 'function',
  inputs: [
    { name: 'collateralToken', type: 'address' },
    { name: 'parentCollectionId', type: 'bytes32' },
    { name: 'conditionId', type: 'bytes32' },
    { name: 'indexSets', type: 'uint256[]' },
  ],
}];

// Throttle: minimum seconds between redeem attempts per user
const MIN_REDEEM_INTERVAL_S = 60;
// Timeout: safety reset after 2 minutes regardless of result
const REDEEM_TIMEOUT_MS = 120_000;

class AutoRedeemer {
  constructor() {
    this._timer = null;
    this._running = false;
    this._polyConnector = null;
    this._redeemLog = [];
    this._maxLog = 200;
    this._lastRedeemTs = {};
    this._redeemRunning = {};
    this._BuilderConfig = null; // cached ESM import
    this._builderCreds = {};    // username → { key, secret, passphrase }
  }

  /**
   * Start the auto-redeemer background loop.
   * @param {object} polyConnector - The Polymarket connector instance
   */
  start(polyConnector) {
    if (this._running) return;
    this._running = true;
    this._polyConnector = polyConnector || null;

    console.log(`[AutoRedeem] started — checking every ${CHECK_INTERVAL_MS / 1000}s for redeemable positions`);

    // Pre-load ESM BuilderConfig asynchronously
    import('@polymarket/builder-signing-sdk').then((mod) => {
      this._BuilderConfig = mod.BuilderConfig;
      console.log('[AutoRedeem] BuilderConfig loaded');
    }).catch((err) => {
      console.error('[AutoRedeem] failed to load BuilderConfig:', err.message);
    });

    // First check after 10s (let everything boot), then every interval
    setTimeout(() => this._checkAll(), 10_000);
    this._timer = setInterval(() => this._checkAll(), CHECK_INTERVAL_MS);
  }

  stop() {
    this._running = false;
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
    console.log('[AutoRedeem] stopped');
  }

  getLog() {
    return this._redeemLog;
  }

  /**
   * Get Builder credentials for a user. Three sources, in priority order:
   *   1. Cached (already resolved for this user)
   *   2. Stored in user store (polyApiKey/Secret/Passphrase — original Builder Codes)
   *   3. Derived via clobClient.createBuilderApiKey() (last resort)
   */
  async _getBuilderCreds(username) {
    // Already cached
    if (this._builderCreds[username]) return this._builderCreds[username];

    const keys = userStore.getKeys(username);

    // Try stored Builder credentials from user store
    // These are the original Builder Codes the user entered (separate from CLOB-derived keys)
    if (keys?.polyApiKey && keys?.polyApiSecret && keys?.polyPassphrase) {
      // Check if these are different from the CLOB-derived keys
      // (CLOB-derived keys are stored on the connector, not in user store)
      const clobKey = this._polyConnector?.apiKey || '';
      const storedKey = keys.polyApiKey;

      if (storedKey !== clobKey) {
        // Stored keys are different from CLOB keys — likely the original Builder Codes
        this._builderCreds[username] = {
          key: storedKey,
          secret: keys.polyApiSecret,
          passphrase: keys.polyPassphrase,
        };
        console.log(`[AutoRedeem] ${username}: using stored Builder keys (key: ${storedKey.slice(0, 8)}...)`);
        return this._builderCreds[username];
      }
    }

    if (this._polyConnector?._clobClient) {
      try {
        console.log(`[AutoRedeem] ${username}: deriving Builder keys via createBuilderApiKey()...`);
        const resp = await this._polyConnector._clobClient.createBuilderApiKey();
        console.log(`[AutoRedeem] ${username}: builder API key created`);

        const creds = this._extractCreds(resp);
        if (creds) {
          this._builderCreds[username] = creds;
          console.log(`[AutoRedeem] ${username}: Builder keys derived (key: ${creds.key.slice(0, 8)}...)`);
          return creds;
        }
      } catch (err) {
        console.error(`[AutoRedeem] ${username}: createBuilderApiKey failed:`, err.message);
      }
    }

    // Last resort: use stored keys even if they match CLOB keys (maybe they work)
    if (keys?.polyApiKey && keys?.polyApiSecret && keys?.polyPassphrase) {
      this._builderCreds[username] = {
        key: keys.polyApiKey,
        secret: keys.polyApiSecret,
        passphrase: keys.polyPassphrase,
      };
      console.log(`[AutoRedeem] ${username}: using stored keys as Builder fallback (key: ${keys.polyApiKey.slice(0, 8)}...)`);
      return this._builderCreds[username];
    }

    return null;
  }

  /**
   * Extract credentials from createBuilderApiKey response (handles multiple formats).
   */
  _extractCreds(resp) {
    if (!resp) return null;

    // Format 1: { key, secret, passphrase }
    if (resp.key && resp.secret && resp.passphrase) {
      return { key: resp.key, secret: resp.secret, passphrase: resp.passphrase };
    }
    // Format 2: { apiKey, secret/apiSecret, passphrase }
    if (resp.apiKey && resp.passphrase) {
      return { key: resp.apiKey, secret: resp.secret || resp.apiSecret, passphrase: resp.passphrase };
    }
    // Format 3: nested { data: { key, secret, passphrase } }
    if (resp.data?.key) {
      return { key: resp.data.key, secret: resp.data.secret, passphrase: resp.data.passphrase };
    }
    return null;
  }

  /**
   * Check all registered users for redeemable positions.
   */
  async _checkAll() {
    if (!this._running) return;

    try {
      const allUsers = userStore.listUsers();
      for (const username of allUsers) {
        try {
          await this._checkUser(username);
        } catch (err) {
          console.debug(`[AutoRedeem] ${username}: ${err.message}`);
        }
      }
    } catch (err) {
      console.error('[AutoRedeem] check cycle error:', err.message);
    }
  }

  /**
   * Check and redeem positions for a single user.
   */
  async _checkUser(username) {
    const keys = userStore.getKeys(username);
    if (!keys || !keys.polyPrivateKey) return;

    // Prevent concurrent redeems for same user
    if (this._redeemRunning[username]) return;

    // Throttle
    const now = Math.floor(Date.now() / 1000);
    if (this._lastRedeemTs[username] && (now - this._lastRedeemTs[username]) < MIN_REDEEM_INTERVAL_S) {
      return;
    }

    // Determine wallet address (proxy wallet holds the positions)
    const walletAddress = keys.polyFunderAddress || (this._polyConnector?.funderAddress) || '';
    if (!walletAddress) return;

    // 1. Fetch user positions via public data-api
    const positions = await this._fetchPositions(walletAddress);
    if (!positions || positions.length === 0) return;

    // 2. Filter for won positions (token price >= 97%)
    const redeemable = positions.filter((p) => {
      const price = parseFloat(p.curPrice ?? p.currentPrice ?? p.price ?? 0);
      const size = parseFloat(p.size ?? p.shares ?? 0);
      if (size <= 0) return false;
      if (p.redeemable !== undefined) return p.redeemable === true && price >= 0.97;
      return price >= 0.98 && (p.resolved === true || p.outcome != null || price >= 0.99);
    });

    if (redeemable.length === 0) return;

    console.log(`[AutoRedeem] ${username}: found ${redeemable.length} redeemable position(s)`);
    this._lastRedeemTs[username] = now;

    // 3. Redeem each position
    this._redeemRunning[username] = true;
    const timeout = setTimeout(() => { this._redeemRunning[username] = false; }, REDEEM_TIMEOUT_MS);

    try {
      for (const pos of redeemable) {
        try {
          await this._redeemPosition(username, pos, keys);
        } catch (err) {
          const condId = (pos.conditionId || pos.condition_id || '').slice(0, 12);
          console.error(`[AutoRedeem] ${username} redeem failed for ${condId}:`, err.message);
        }
      }
    } finally {
      clearTimeout(timeout);
      this._redeemRunning[username] = false;
    }
  }

  /**
   * Fetch user positions via public Polymarket data-api.
   */
  async _fetchPositions(walletAddress) {
    try {
      const url = `${DATA_API_BASE}/positions?user=${walletAddress}&sizeThreshold=.01`;
      const opts = { timeout: 15_000 };
      const resp = await axios.get(url, opts);
      return Array.isArray(resp.data) ? resp.data : [];
    } catch (err) {
      if (err.response?.status === 404) return [];
      console.debug(`[AutoRedeem] fetchPositions: ${err.message}`);
      return [];
    }
  }

  /**
   * Redeem a resolved/won position via the Polymarket Relayer.
   */
  async _redeemPosition(username, position, keys) {
    const conditionId = position.conditionId || position.condition_id;
    if (!conditionId) return;

    const size = parseFloat(position.size ?? position.shares ?? 0);
    const price = parseFloat(position.curPrice ?? position.currentPrice ?? position.price ?? 0);

    console.log(`[AutoRedeem] ${username}: redeeming ${conditionId.slice(0, 12)}... (size: ${size.toFixed(2)}, price: ${price.toFixed(3)})`);

    if (!this._BuilderConfig) {
      console.error(`[AutoRedeem] ${username}: BuilderConfig not loaded yet — retrying next cycle`);
      return;
    }

    // Get Builder credentials (stored or derived)
    const builderCreds = await this._getBuilderCreds(username);
    if (!builderCreds) {
      console.error(`[AutoRedeem] ${username}: no Builder credentials available — cannot redeem`);
      return;
    }

    try {
      // 1. Create BuilderConfig with Builder credentials
      const builderConfig = new this._BuilderConfig({
        localBuilderCreds: builderCreds,
      });

      // 2. Create viem wallet client (for signing)
      const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
      const account = privateKeyToAccount(pk);
      const viemWallet = createWalletClient({ account, chain: polygon, transport: http() });

      // 3. Create RelayClient (SAFE type for proxy wallets)
      const { RelayClient, RelayerTxType } = require('@polymarket/builder-relayer-client');
      const client = new RelayClient(RELAYER_URL, 137, viemWallet, builderConfig, RelayerTxType.SAFE);

      // 4. Encode CTF.redeemPositions calldata
      const redeemCalldata = encodeFunctionData({
        abi: CTF_REDEEM_ABI,
        functionName: 'redeemPositions',
        args: [PUSD_ADDRESS, ZERO_HASH, conditionId, [1n, 2n]],
      });

      // 5. Execute via relayer with retry
      const txns = [{ to: CTF_CONTRACT, data: redeemCalldata, value: '0' }];
      let txResponse = null;
      const MAX_RETRIES = 3;
      const RETRY_DELAYS = [3000, 6000, 12000];

      for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        try {
          console.log(`[AutoRedeem] ${username}: submitting redeem via relayer (attempt ${attempt + 1}/${MAX_RETRIES})...`);
          txResponse = await client.execute(txns, 'Redeem positions');
          break;
        } catch (execErr) {
          const errMsg = execErr.message || '';
          const isRetryable = errMsg.includes('522') || errMsg.includes('Timeout') ||
                              errMsg.includes('connection error') || errMsg.includes('ETIMEDOUT');

          if (isRetryable && attempt < MAX_RETRIES - 1) {
            console.warn(`[AutoRedeem] ${username}: relayer attempt ${attempt + 1} failed — retrying in ${RETRY_DELAYS[attempt] / 1000}s...`);
            await new Promise((r) => setTimeout(r, RETRY_DELAYS[attempt]));
          } else if (errMsg.includes('401') || errMsg.includes('Unauthorized')) {
            // Auth failed — clear cached creds so we try again next cycle
            console.error(`[AutoRedeem] ${username}: relayer 401 — clearing cached Builder creds`);
            delete this._builderCreds[username];
            throw execErr;
          } else {
            throw execErr;
          }
        }
      }

      if (!txResponse) throw new Error('relayer execute returned null');

      // 6. Wait for on-chain confirmation
      console.log(`[AutoRedeem] ${username}: waiting for on-chain confirmation...`);
      const confirmed = await txResponse.wait();

      if (confirmed) {
        const txHash = confirmed.transactionHash || txResponse.transactionHash || 'confirmed';
        this._logRedeem(username, conditionId, size, 'relayer', 'success', txHash);
        console.log(`[AutoRedeem] ${username}: redeemed via relayer — tx: ${txHash}`);
      } else {
        const txId = txResponse.transactionID || 'unknown';
        this._logRedeem(username, conditionId, size, 'relayer', 'failed', txId, 'tx failed or timed out');
        console.warn(`[AutoRedeem] ${username}: relayer tx failed or timed out (txId: ${txId})`);
      }
    } catch (err) {
      this._logRedeem(username, conditionId, size, 'relayer', 'failed', null, err.message);
      console.error(`[AutoRedeem] ${username}: relayer redeem failed:`, err.message);
    }
  }

  _logRedeem(username, conditionId, size, method, status, txHash = null, error = null) {
    const entry = {
      timestamp: new Date().toISOString(),
      username,
      conditionId: conditionId.slice(0, 16) + '...',
      size: size.toFixed(2),
      method,
      status,
    };
    if (txHash) entry.txHash = txHash;
    if (error) entry.error = error.slice(0, 200);
    this._redeemLog.push(entry);
    if (this._redeemLog.length > this._maxLog) {
      this._redeemLog = this._redeemLog.slice(-this._maxLog);
    }
  }
}

module.exports = AutoRedeemer;
