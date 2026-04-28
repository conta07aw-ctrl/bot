/**
 * Capital Guard — replaces static bankroll with real balance checking.
 *
 * Instead of a fake "bankroll" number, this:
 *   1. Checks REAL balances on Kalshi and Polymarket
 *   2. Blocks trades if either platform balance drops below user's stop threshold
 *   3. Calculates exact entry size (fixed $ per trade, not Kelly)
 *   4. Caches balances to avoid hammering APIs (refreshes every 30s)
 *
 * User settings:
 *   - entrySize:        exact $ per trade (e.g. $10 = always $10)
 *   - kalshiStopAt:     stop trading if Kalshi balance < this (e.g. $50)
 *   - polyStopAt:       stop trading if Polymarket balance < this (e.g. $50)
 */

const axios = require('axios');
const { ethers } = require('ethers');
const { HttpsProxyAgent } = require('https-proxy-agent');

// Polygon RPC — multiple fallbacks in case one goes down.
// List curated for reliability — ordered by observed uptime.
// Removed 2026-04: 1rpc.io/matic (TLS drops) and polygon.meowrpc.com (404 dead).
const POLYGON_RPCS = [
  'https://polygon-rpc.com',                  // official Polygon RPC
  'https://polygon.llamarpc.com',             // LlamaRPC — also used by usdcApproval
  'https://polygon-bor-rpc.publicnode.com',   // PublicNode
  'https://polygon.gateway.tenderly.co',      // Tenderly
  'https://rpc.ankr.com/polygon',             // Ankr
];
// pUSD on Polygon (CLOB v2 collateral token)
const USDC_ADDRESS = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB';
const USDC_ABI = ['function balanceOf(address) view returns (uint256)'];

const BALANCE_CACHE_MS = 10_000; // refresh balances every 10s

class CapitalGuard {
  constructor(kalshi, polymarket) {
    this.kalshi = kalshi;
    this.polymarket = polymarket;

    // User-configurable thresholds (set via settings)
    this.entrySize = 10;        // exact $ per trade
    this.kalshiStopAt = 0;      // 0 = no stop
    this.polyStopAt = 0;        // 0 = no stop

    // Cached balances
    this._balances = { kalshi: null, polymarket: null, lastFetch: 0 };

    // Reusable Polygon RPC providers — created once, used for every balance
    // fetch. Previously a new provider was created per RPC per call (5 × every
    // 60s + after each trade), leaking sockets/timers/listeners indefinitely.
    const providerOptions = process.env.POLY_PROXY_URL ? { httpsAgent: new HttpsProxyAgent(process.env.POLY_PROXY_URL) } : {};
    this._polygonProviders = POLYGON_RPCS.map(rpcUrl =>
      new ethers.JsonRpcProvider(rpcUrl, 137, {
        staticNetwork: true,
        batchMaxCount: 1,
        ...providerOptions,
      })
    );

    // Background refresh state. Started via start() — keeps the dispatcher
    // critical path (canTrade) completely free of network I/O.
    this._refreshTimer = null;
    this._refreshIntervalMs = 60_000;

    // Trade tracking (for P&L display)
    this.totalPnl = 0;
    this.tradeCount = 0;
  }

  /**
   * Start background balance refresh.
   *
   * Architecture: balance fetches (Polygon RPC + Kalshi REST) used to happen
   * synchronously inside canTrade() on the dispatcher's critical path. With a
   * 30s cache, every signal arriving more than 30s after the last one paid a
   * 500-2800ms penalty to refresh the cache — long enough for the market to
   * move and kill the arb before the order was even sent.
   *
   * This moves the fetch off the critical path entirely: a background timer
   * refreshes the cache every 60s, and canTrade()/getCachedBalances() become
   * pure synchronous reads. Trade-off: up to 60s of staleness on stop
   * thresholds, acceptable because entrySize is small and the worst case is
   * one extra trade before the circuit breaker catches up on the next tick.
   */
  start() {
    // Prime cache now (non-blocking) so the first trade doesn't fire against
    // empty balances. The dispatcher safely treats null as "unknown, allow".
    this.refreshBalances().catch((e) =>
      console.warn('[CapitalGuard] initial balance refresh failed:', e.message),
    );

    if (this._refreshTimer) clearInterval(this._refreshTimer);
    this._refreshTimer = setInterval(() => {
      this.refreshBalances().catch(() => {}); // swallow, keep last known good
    }, this._refreshIntervalMs);
    if (this._refreshTimer.unref) this._refreshTimer.unref();

    console.log(
      `[CapitalGuard] background refresh started (every ${this._refreshIntervalMs / 1000}s)`,
    );
  }

  stop() {
    if (this._refreshTimer) {
      clearInterval(this._refreshTimer);
      this._refreshTimer = null;
    }
  }

  /**
   * Update thresholds from user settings.
   */
  configure({ entrySize, kalshiStopAt, polyStopAt }) {
    if (entrySize != null) this.entrySize = entrySize;
    if (kalshiStopAt != null) this.kalshiStopAt = kalshiStopAt;
    if (polyStopAt != null) this.polyStopAt = polyStopAt;
  }

  /**
   * Check if trading is allowed based on current CACHED balances.
   *
   * SYNCHRONOUS — reads cached values only, never performs network I/O.
   * Safe on the dispatcher's critical path. The cache is kept warm by the
   * background timer started in start() and by an extra refresh triggered
   * after each trade (via dispatcher._recordTrade).
   *
   * Returns { ok: boolean, reason?: string, balances: { kalshi, polymarket } }
   */
  canTrade() {
    const balances = this.getCachedBalances();

    if (this.kalshiStopAt > 0 && balances.kalshi !== null && balances.kalshi < this.kalshiStopAt) {
      return {
        ok: false,
        reason: `Kalshi balance ($${balances.kalshi.toFixed(2)}) below stop threshold ($${this.kalshiStopAt})`,
        balances,
      };
    }

    if (this.polyStopAt > 0 && balances.polymarket !== null && balances.polymarket < this.polyStopAt) {
      return {
        ok: false,
        reason: `Polymarket balance ($${balances.polymarket.toFixed(2)}) below stop threshold ($${this.polyStopAt})`,
        balances,
      };
    }

    return { ok: true, balances };
  }

  /**
   * Calculate position sizing based on fixed entry size.
   * Returns { units, betSize } or null if entry size too small.
   */
  size(totalCostPerUnit) {
    if (totalCostPerUnit <= 0 || this.entrySize <= 0) return null;

    const units = Math.floor(this.entrySize / totalCostPerUnit);
    if (units <= 0) return null;

    const betSize = Math.round(units * totalCostPerUnit * 100) / 100;

    return { units, betSize };
  }

  /**
   * Record a trade result for P&L tracking.
   */
  recordTrade(trade) {
    this.totalPnl += trade.pnl || 0;
    this.totalPnl = Math.round(this.totalPnl * 100) / 100;
    this.tradeCount++;
  }

  /**
   * Get CACHED balances. No network I/O. Background refresh keeps this
   * up to date (every 60s + after each trade).
   *
   * Callers who truly need a fresh pull must call refreshBalances()
   * explicitly — never from a critical path.
   */
  async getBalances() {
    return this.getCachedBalances();
  }

  /**
   * Synchronous getter for cached balances. Safe on critical path.
   */
  getCachedBalances() {
    return { kalshi: this._balances.kalshi, polymarket: this._balances.polymarket };
  }

  /**
   * Fetch fresh balances from both platforms in parallel and update the
   * cache. Called by the background timer (every 60s) and once after each
   * trade via dispatcher._recordTrade. Must NEVER be awaited from the
   * dispatcher critical path — fire-and-forget only.
   */
  async refreshBalances() {
    const [kalshi, poly] = await Promise.allSettled([
      this._fetchKalshiBalance(),
      this._fetchPolyBalance(),
    ]);

    if (kalshi.status === 'fulfilled') {
      this._balances.kalshi = kalshi.value;
    }
    if (poly.status === 'fulfilled') {
      this._balances.polymarket = poly.value;
    }
    this._balances.lastFetch = Date.now();

    return this.getCachedBalances();
  }

  /**
   * Fetch Kalshi portfolio balance via REST API.
   */
  async _fetchKalshiBalance() {
    if (!this.kalshi.connected) return null;

    try {
      const data = await this.kalshi._get('/trade-api/v2/portfolio/balance');
      // Kalshi returns balance in cents
      const balanceCents = data.balance ?? data.available_balance ?? 0;
      return balanceCents / 100;
    } catch (err) {
      console.debug('[CapitalGuard] Kalshi balance fetch error:', err.message);
      return null;
    }
  }

  /**
   * Fetch PUSD balance from Polygon by RACING all RPCs in parallel.
   *
   * Why parallel instead of sequential fallback:
   *   The old code did one RPC at a time with 8s timeout each — worst case
   *   ~40s of stall holding the dispatcher mutex, which starved the signal
   *   pipeline and made signal ages balloon to 11-30s. Parallel race with a
   *   tight 2.5s timeout means: fastest wins, slow/dead RPCs are ignored,
   *   total wall time is capped at 2.5s even if 4 out of 5 RPCs are down.
   */
  async _fetchPusdBalance(address) {
    if (!address) return null;

    const TIMEOUT_MS = 2500;

    const attempts = this._polygonProviders.map((provider, idx) => new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timeout RPC#${idx}`)), TIMEOUT_MS);
      (async () => {
        try {
          const pusd = new ethers.Contract(PUSD_ADDRESS, PUSD_ABI, provider);
          const raw = await pusd.balanceOf(address);
          clearTimeout(timer);
          resolve(parseFloat(ethers.formatUnits(raw, 6)));
        } catch (err) {
          clearTimeout(timer);
          reject(err);
        }
      })();
    }));

    try {
      return await Promise.any(attempts);
    } catch (aggErr) {
      console.debug('[CapitalGuard] all Polygon RPCs failed (parallel race):', aggErr.message);
      return null;
    }
  }

  /**
   * Fetch Polymarket PUSD balance from proxy wallet on Polygon.
   */
  async _fetchPolyBalance() {
    return this._fetchPusdBalance(this.polymarket.funderAddress);
  }

  /**
   * Get balances for a specific user using their own keys (not global connectors).
   */
  async getBalancesForUser(keys) {
    const [kalshi, poly] = await Promise.allSettled([
      this._fetchKalshiBalanceForUser(keys),
      this._fetchPolyBalanceForUser(keys),
    ]);

    return {
      kalshi: kalshi.status === 'fulfilled' ? kalshi.value : null,
      polymarket: poly.status === 'fulfilled' ? poly.value : null,
    };
  }

  async _fetchKalshiBalanceForUser(keys) {
    if (!keys?.kalshiApiKey || !keys?.kalshiPrivateKeyPem) return null;

    try {
      // Use the kalshi connector's signed request with user's keys
      const data = await this.kalshi._getWithKeys(
        '/trade-api/v2/portfolio/balance',
        keys.kalshiApiKey,
        keys.kalshiPrivateKeyPem
      );
      const balanceCents = data.balance ?? data.available_balance ?? 0;
      return balanceCents / 100;
    } catch (err) {
      console.debug('[CapitalGuard] user Kalshi balance error:', err.message);
      return null;
    }
  }

  async _fetchPolyBalanceForUser(keys) {
    return this._fetchPusdBalance(keys?.polyFunderAddress);
  }

  /**
   * Get state for API / dashboard.
   */
  getState() {
    return {
      entrySize: this.entrySize,
      kalshiStopAt: this.kalshiStopAt,
      polyStopAt: this.polyStopAt,
      balances: {
        kalshi: this._balances.kalshi,
        polymarket: this._balances.polymarket,
      },
      totalPnl: this.totalPnl,
      tradeCount: this.tradeCount,
    };
  }
}

module.exports = CapitalGuard;
