/**
 * Global monitor — EVENT-DRIVEN via WebSocket streams.
 *
 * Architecture:
 *   1. REST discovery: finds current 15-min markets on startup
 *   2. WebSocket subscription: streams real-time prices from both platforms
 *   3. On EVERY price tick → immediately runs decision engine (sub-millisecond reaction)
 *   4. Discovery timer: every 30s checks if a new 15-min window opened → re-discovers & re-subscribes
 *
 * No more 15s polling. Every price movement triggers instant arbitrage analysis.
 */

const EventEmitter = require('events');
const { ASSETS } = require('../config/assets');
const userStore = require('../store/users');

const DISCOVERY_INTERVAL_MS = 30_000; // check for new 15-min windows every 30s

class Monitor extends EventEmitter {
  constructor(kalshi, polymarket, decisionEngine, dispatcher, rtdsClient) {
    super();
    this.kalshi = kalshi;
    this.polymarket = polymarket;
    this.decisionEngine = decisionEngine || null;
    this.dispatcher = dispatcher || null;
    this.rtdsClient = rtdsClient || null;
    this.running = false;
    this._discoveryTimer = null;
    this._currentWindowTs = 0; // tracks current 15-min window to detect rotation

    /**
     * In-memory price snapshot.
     * Structure:  { BTC: { kalshi: [...markets], polymarket: [...markets] }, ... }
     */
    this.prices = {};

    /**
     * Detected opportunities (from Decision Engine).
     */
    this.opportunities = [];

    /**
     * Track executed trades per asset per round to enforce 1-trade-per-asset-per-round.
     * Key: "{SYMBOL}-{roundOpenTs}", Value: 'success' | 'pending'
     * On success → locked permanently for that round.
     * On failure → deleted, allowing retry after cooldown.
     */
    this._executedThisRound = new Map();
    this._lastFailTs = {};  // symbol → timestamp of last failed attempt (retry cooldown)

    // Bind tick handlers
    this._onKalshiTick = this._onKalshiTick.bind(this);
    this._onPolyTick = this._onPolyTick.bind(this);
  }

  /* ---------------------------------------------------------- */
  /*  Helpers                                                    */
  /* ---------------------------------------------------------- */

  getEnabledAssets() {
    // Merge static config with user filters (first user's settings)
    const users = userStore.listUsers();
    const filters = users.length > 0 ? userStore.getFilters(users[0]) : null;

    return Object.values(ASSETS).filter((a) => {
      if (filters && filters[a.symbol] != null) {
        // User has a filter for this asset
        const f = filters[a.symbol];
        return typeof f === 'object' ? f.enabled !== false : f !== false;
      }
      return a.enabled;
    });
  }

  /**
   * Get user filters for decision engine configuration.
   */
  _getUserFilters() {
    const users = userStore.listUsers();
    return users.length > 0 ? userStore.getFilters(users[0]) : null;
  }

  getSnapshot() {
    return {
      prices: this.prices,
      opportunities: this.opportunities,
      lastPoll: this._lastUpdate || null,
      polling: this.running,
    };
  }

  _getCurrentWindowTs() {
    return Math.floor(Date.now() / 1000 / 900) * 900;
  }

  /**
   * Check if Kalshi markets look settled (prices near 0 or 1).
   * Returns true if most markets appear resolved.
   */
  _checkIfKalshiSettled() {
    let settled = 0;
    let total = 0;
    for (const sym of Object.keys(this.prices)) {
      const kalshi = this.prices[sym]?.kalshi || [];
      for (const m of kalshi) {
        total++;
        if (m.yesAsk >= 0.95 || m.yesAsk <= 0.05) settled++;
      }
    }
    return total > 0 && settled / total > 0.5;
  }

  /* ---------------------------------------------------------- */
  /*  Start / Stop                                               */
  /* ---------------------------------------------------------- */

  async start() {
    this.running = true;
    const assets = this.getEnabledAssets();
    console.log('[Monitor] starting — real-time mode (WebSocket + REST fallback)');
    console.log('[Monitor] tracking:', assets.map((a) => a.symbol).join(', '));

    // 1. Initial REST discovery to find current markets
    await this._discoverMarkets();

    // 2. Attach WebSocket tick listeners
    this.kalshi.on('tick', this._onKalshiTick);
    this.polymarket.on('tick', this._onPolyTick);

    // 2b. Listen for RTDS strike captures (may arrive after initial discovery)
    if (this.rtdsClient) {
      this._onRtdsStrike = this._onRtdsStrike.bind(this);
      this.rtdsClient.on('strike', this._onRtdsStrike);
    }

    // 3. Periodic discovery to catch new 15-min windows (every 15s)
    this._discoveryTimer = setInterval(() => this._checkWindowRotation(), 15_000);

    // 4. Kalshi REST price refresh (every 15s)
    //    After round rotation, new markets may have no WS quotes for minutes.
    //    REST polling ensures we pick up prices as soon as the orderbook populates.
    this._kalshiPriceTimer = setInterval(() => this._refreshKalshiPrices(), 15_000);

    // 5. WS health monitor — detect silent disconnects (every 60s)
    this._lastKalshiTick = Date.now();
    this._lastPolyTick = Date.now();
    this._wsHealthTimer = setInterval(() => this._checkWsHealth(), 60_000);

    console.log('[Monitor] live — reacting to every price tick in real-time');
  }

  stop() {
    this.running = false;
    if (this._discoveryTimer) {
      clearInterval(this._discoveryTimer);
      this._discoveryTimer = null;
    }
    if (this._kalshiPriceTimer) {
      clearInterval(this._kalshiPriceTimer);
      this._kalshiPriceTimer = null;
    }
    if (this._wsHealthTimer) {
      clearInterval(this._wsHealthTimer);
      this._wsHealthTimer = null;
    }
    this.kalshi.removeListener('tick', this._onKalshiTick);
    this.polymarket.removeListener('tick', this._onPolyTick);
    if (this.rtdsClient && this._onRtdsStrike) {
      this.rtdsClient.removeListener('strike', this._onRtdsStrike);
    }
    console.log('[Monitor] stopped');
  }

  /* ---------------------------------------------------------- */
  /*  WebSocket health monitoring                                */
  /* ---------------------------------------------------------- */

  _checkWsHealth() {
    if (!this.running) return;

    const now = Date.now();
    const kalshiAge = Math.round((now - (this._lastKalshiTick || 0)) / 1000);
    const polyAge = Math.round((now - (this._lastPolyTick || 0)) / 1000);
    const STALE_THRESHOLD = 120; // 2 minutes without ticks = stale

    if (kalshiAge > STALE_THRESHOLD) {
      console.warn(`[Monitor] WS STALE: Kalshi — no tick for ${kalshiAge}s. Reconnecting...`);
      this.kalshi.destroy();
      const tickers = [...this.kalshi._subscribedTickers];
      // Fire-and-forget — the watchdog isn't awaiting a scan right after,
      // so a synchronous call is fine. The internal Promise handles its
      // own warm-start.
      if (tickers.length > 0) this.kalshi.subscribeWs(tickers).catch(() => {});
    }
    if (polyAge > STALE_THRESHOLD) {
      console.warn(`[Monitor] WS STALE: Polymarket — no tick for ${polyAge}s. Reconnecting...`);
      this.polymarket.destroy();
      const markets = [...this.polymarket._subscribedAssets.values()];
      if (markets.length > 0) this.polymarket.subscribeWs(markets);
    }

    // Log WS health status every check
    const kalshiStatus = kalshiAge <= STALE_THRESHOLD ? 'OK' : 'STALE';
    const polyStatus = polyAge <= STALE_THRESHOLD ? 'OK' : 'STALE';
    console.log(`[Monitor] WS health: Kalshi=${kalshiStatus} (${kalshiAge}s ago), Poly=${polyStatus} (${polyAge}s ago)`);
  }

  /* ---------------------------------------------------------- */
  /*  Market discovery (REST) + WebSocket subscription            */
  /* ---------------------------------------------------------- */

  async _discoverMarkets() {
    const assets = this.getEnabledAssets();
    const ts = new Date().toISOString();
    this._lastUpdate = ts;
    this._currentWindowTs = this._getCurrentWindowTs();

    // CLEAR old prices/opportunities — stale data causes false arbitrage
    this.prices = {};
    this.opportunities = [];

    const allKalshiTickers = [];
    const allPolyMarkets = [];

    for (const asset of assets) {
      try {
        const [kalshiMarkets, polyMarkets] = await Promise.all([
          this.kalshi.getMarkets(asset),
          this.polymarket.getMarkets(asset),
        ]);

        this.prices[asset.symbol] = {
          kalshi: kalshiMarkets,
          polymarket: polyMarkets,
          updatedAt: ts,
        };

        // Inject RTDS strike into Polymarket markets
        if (this.rtdsClient) {
          const roundTs = this._currentWindowTs;
          const strike = this.rtdsClient.getStrike(asset.symbol, roundTs);
          for (const m of polyMarkets) {
            m.strike = strike; // null if not captured yet → blocks trading
          }
          if (strike != null) {
            const dec = strike >= 1000 ? 2 : strike >= 10 ? 4 : 5;
            console.log(`[Monitor] ${asset.symbol} Polymarket strike from RTDS: $${strike.toFixed(dec)}`);
          } else {
            console.warn(`[Monitor] ${asset.symbol} Polymarket strike NOT available from RTDS — trading blocked for this round`);
          }
        }

        // Collect tickers/IDs for WebSocket subscription
        for (const m of kalshiMarkets) allKalshiTickers.push(m.ticker);
        for (const m of polyMarkets) allPolyMarkets.push(m);

        const kCount = kalshiMarkets.length;
        const pCount = polyMarkets.length;
        if (kCount > 0 || pCount > 0) {
          console.log(`[Monitor] ${asset.symbol} — Kalshi: ${kCount} strikes, Polymarket: ${pCount} markets`);
        }

        // Emit fresh prices
        this.emit('prices', {
          asset: asset.symbol,
          kalshi: kalshiMarkets,
          polymarket: polyMarkets,
          timestamp: ts,
        });

        // Run initial arbitrage scan
        if (this.decisionEngine) {
          this._runScan(asset.symbol, kalshiMarkets, polyMarkets);
        }
      } catch (err) {
        console.error(`[Monitor] discovery error for ${asset.symbol}:`, err.message);
      }
    }

    // Subscribe to WebSocket streams. Kalshi subscribe is awaited because
    // it warm-starts the orderbook mini-book from a real REST fetch, and the
    // decisionEngine relies on orderbook data (NOT the /markets endpoint's
    // stale quotes) for accurate top-of-book. Without this await, the first
    // scan after discovery races against the warm-start and sees last-trade
    // quotes instead of the live orderbook top.
    if (allKalshiTickers.length > 0) {
      await this.kalshi.subscribeWs(allKalshiTickers);
    }
    if (allPolyMarkets.length > 0) {
      this.polymarket.subscribeWs(allPolyMarkets);
    }

    console.log(`[Monitor] subscribed — Kalshi: ${allKalshiTickers.length} tickers, Polymarket: ${allPolyMarkets.length} markets`);
  }

  /**
   * Check if the 15-min window has rotated. If so, discover new markets and resubscribe.
   */
  async _checkWindowRotation() {
    if (!this.running) return;

    const currentWindow = this._getCurrentWindowTs();
    if (currentWindow === this._currentWindowTs) return;

    const __rotT0 = Date.now();
    console.log(`[Monitor] === NEW 15-MIN WINDOW === (${this._currentWindowTs} → ${currentWindow})`);
    // Instrumentation: report end-to-end wall time of the rotation. Most of
    // this is async (REST calls), but any sync blocking inside shows up here.
    setImmediate(() => {
      const __rotMs = Date.now() - __rotT0;
      if (__rotMs > 200) {
        console.warn(`[Perf] window rotation total: ${__rotMs}ms (>200ms)`);
      }
    });

    // Reset per-round trade tracking
    this._executedThisRound.clear();
    this._lastFailTs = {};

    // Clear early exit positions — contracts from the old round settle at expiry
    if (this.dispatcher && this.dispatcher._openPositions.length > 0) {
      console.log(`[EarlyExit] window rotation — clearing ${this.dispatcher._openPositions.length} position(s)`);
      this.dispatcher._openPositions = [];
    }

    // ── Step 1: Stop everything from the old round ──
    const oldKalshiTickers = [...this.kalshi.markets.keys()];
    if (oldKalshiTickers.length > 0) {
      this.kalshi.unsubscribeTickers(oldKalshiTickers);
    }
    const oldPolyConditions = [...this.polymarket._subscribedAssets.keys()];
    if (oldPolyConditions.length > 0) {
      this.polymarket.unsubscribeMarkets(oldPolyConditions);
    }
    this.kalshi.markets.clear();
    this.polymarket.markets.clear();
    this.prices = {};
    this.opportunities = [];
    this._currentWindowTs = currentWindow;
    this.emit('prices', { cleared: true, timestamp: new Date().toISOString() });

    // ── Step 2: Polymarket strikes come from RTDS automatically ──
    // RTDS captures strikes via _processTick: first price in the first 30s of round.
    // No manual capture needed — RTDS is continuously running.
    // Kalshi strikes come with market data (floor_strike) in Step 3.
    console.log('[Monitor] step 2: RTDS capturing Polymarket strikes (automatic)...');

    // ── Step 3: Discover markets (REST) — Kalshi strikes arrive here via floor_strike ──
    console.log('[Monitor] step 3: waiting 5s then discovering markets...');
    await new Promise((r) => setTimeout(r, 5000));
    await this._discoverMarkets();

    // ── Step 4: Retry if Kalshi has no markets yet (post-rotation delay) ──
    const retryDelays = [8_000, 12_000];
    for (const delay of retryDelays) {
      const hasNoKalshi = Object.values(this.prices).some(
        (p) => p.kalshi.length === 0
      );
      const kalshiSettled = this._checkIfKalshiSettled();

      if (!hasNoKalshi && !kalshiSettled) break;

      const reason = hasNoKalshi ? '0 markets' : 'settled markets';
      console.log(`[Monitor] Kalshi has ${reason} — retrying in ${delay / 1000}s...`);
      await new Promise((r) => setTimeout(r, delay));
      this.kalshi.markets.clear();
      await this._discoverMarkets();
    }

    // ── Step 5: Tickers loaded, WS subscribed — arbitrage evaluation starts ──
    console.log('[Monitor] round ready — strikes captured, tickers loaded, scanning active');
  }

  /**
   * Refresh Polymarket prices via REST (fallback for when WS doesn't deliver).
   */
  async _refreshKalshiPrices() {
    if (!this.running) return;

    const assets = this.getEnabledAssets();
    const ts = new Date().toISOString();

    for (const asset of assets) {
      try {
        const cached = this.prices[asset.symbol];
        if (!cached) continue;

        // Refresh if: (a) 0 Kalshi markets (post-rotation), or (b) existing markets have empty prices
        const hasNoMarkets = cached.kalshi.length === 0;
        const hasEmptyPrices = cached.kalshi.some((m) => m.yesAsk === 0);
        if (!hasNoMarkets && !hasEmptyPrices) continue;

        const kalshiMarkets = await this.kalshi.getMarkets(asset);
        if (!kalshiMarkets.length) continue;

        // Check if any prices actually populated
        const hasNewPrices = kalshiMarkets.some((m) => m.yesAsk > 0);
        if (!hasNewPrices && !hasNoMarkets) continue;

        cached.kalshi = kalshiMarkets;
        cached.updatedAt = ts;
        this._lastUpdate = ts;

        // Re-subscribe WS for new tickers. We AWAIT this so the orderbook
        // warm-start completes before we run _runScan below — otherwise the
        // scan uses the /markets endpoint's stale quotes instead of the live
        // orderbook top-of-book, and the dispatcher pre-check aborts every
        // trade with a ~9¢ gap.
        const tickers = kalshiMarkets.map((m) => m.ticker);
        await this.kalshi.subscribeWs(tickers);

        // Also inject RTDS strikes into Polymarket markets (may have arrived after initial discovery)
        if (this.rtdsClient && cached.polymarket) {
          const strike = this.rtdsClient.getStrike(asset.symbol, this._currentWindowTs);
          if (strike != null) {
            for (const m of cached.polymarket) {
              if (m.strike == null) m.strike = strike;
            }
          }
        }

        console.log(`[Monitor] ${asset.symbol} Kalshi ${hasNoMarkets ? 'markets discovered' : 'prices refreshed'} via REST (${kalshiMarkets.length} strikes)`);

        this.emit('prices', {
          asset: asset.symbol,
          kalshi: kalshiMarkets,
          polymarket: cached.polymarket,
          timestamp: ts,
        });

        if (this.decisionEngine) {
          this._runScan(asset.symbol, kalshiMarkets, cached.polymarket);
        }
      } catch (_) {}
    }
  }


  /* ---------------------------------------------------------- */
  /*  Real-time tick handlers                                    */
  /* ---------------------------------------------------------- */

  _onKalshiTick(market) {
    if (!this.running || !market.asset) return;
    this._lastKalshiTick = Date.now();

    const symbol = market.asset;
    const ts = new Date().toISOString();
    this._lastUpdate = ts;

    // Update cached prices
    if (!this.prices[symbol]) this.prices[symbol] = { kalshi: [], polymarket: [], updatedAt: ts };
    const cached = this.prices[symbol];
    const idx = cached.kalshi.findIndex((m) => m.ticker === market.ticker);
    if (idx >= 0) cached.kalshi[idx] = market;
    else cached.kalshi.push(market);
    cached.updatedAt = ts;

    // Emit price update
    this.emit('prices', {
      asset: symbol,
      kalshi: cached.kalshi,
      polymarket: cached.polymarket,
      timestamp: ts,
    });

    // Instant arbitrage scan
    if (this.decisionEngine) {
      this._runScan(symbol, cached.kalshi, cached.polymarket);
    }
  }

  _onPolyTick(market) {
    if (!this.running || !market.asset) return;
    this._lastPolyTick = Date.now();

    const symbol = market.asset;
    const ts = new Date().toISOString();
    this._lastUpdate = ts;

    // Update cached prices
    if (!this.prices[symbol]) this.prices[symbol] = { kalshi: [], polymarket: [], updatedAt: ts };
    const cached = this.prices[symbol];
    const idx = cached.polymarket.findIndex((m) => m.conditionId === market.conditionId);
    if (idx >= 0) cached.polymarket[idx] = market;
    else cached.polymarket.push(market);
    cached.updatedAt = ts;

    // Emit price update
    this.emit('prices', {
      asset: symbol,
      kalshi: cached.kalshi,
      polymarket: cached.polymarket,
      timestamp: ts,
    });

    // Instant arbitrage scan
    if (this.decisionEngine) {
      this._runScan(symbol, cached.kalshi, cached.polymarket);
    }
  }

  /**
   * Handle late RTDS strike capture — update Polymarket markets and re-scan.
   */
  _onRtdsStrike({ symbol, price, roundOpenTs }) {
    if (!this.running) return;

    // Only apply if this strike is for the current round
    if (roundOpenTs !== this._currentWindowTs) return;

    const cached = this.prices[symbol];
    if (!cached || !cached.polymarket) return;

    // Inject strike into all Polymarket markets for this symbol
    let updated = false;
    for (const m of cached.polymarket) {
      if (m.strike == null) {
        m.strike = price;
        updated = true;
      }
    }

    if (updated) {
      const dec = price >= 1000 ? 2 : price >= 10 ? 4 : 5;
      console.log(`[Monitor] ${symbol} Polymarket strike updated from RTDS: $${price.toFixed(dec)} — re-scanning`);

      const ts = new Date().toISOString();
      this.emit('prices', {
        asset: symbol,
        kalshi: cached.kalshi,
        polymarket: cached.polymarket,
        timestamp: ts,
      });

      // Re-run arbitrage scan now that we have the strike
      if (this.decisionEngine) {
        this._runScan(symbol, cached.kalshi, cached.polymarket);
      }
    }

  }

  /* ---------------------------------------------------------- */
  /*  Arbitrage scan + dispatch                                  */
  /* ---------------------------------------------------------- */

  _runScan(symbol, kalshiMarkets, polyMarkets) {
    // Build per-asset overrides from user filters
    const filters = this._getUserFilters();
    const assetFilter = filters?.[symbol];
    const overrides = {};
    if (assetFilter && typeof assetFilter === 'object') {
      if (assetFilter.minRoiPct != null) overrides.minRoiPct = assetFilter.minRoiPct;
      if (assetFilter.maxStrikeDiff != null) overrides.maxStrikeDiff = assetFilter.maxStrikeDiff;
      if (assetFilter.entrySize != null) overrides.entrySize = assetFilter.entrySize;
    }

    const opps = this.decisionEngine.scan(symbol, kalshiMarkets, polyMarkets, overrides);

    // Keep only the BEST opportunity per asset (highest ROI) — no duplicate legs
    let bestOpp = null;
    for (const o of opps) {
      if (!bestOpp || o.roiPct > bestOpp.roiPct) bestOpp = o;
    }

    const filtered = bestOpp ? [bestOpp] : [];

    // Update opportunities list (replace any existing for this asset)
    this.opportunities = [
      ...this.opportunities.filter((o) => o.asset !== symbol),
      ...filtered,
    ];
    this.emit('opportunities', this.opportunities);

    // Auto-dispatch — one trade per asset per round, retry allowed on failure
    if (this.dispatcher && bestOpp && bestOpp.execute) {
      const roundKey = `${symbol}-${this._currentWindowTs}`;
      if (this._executedThisRound.get(roundKey) === 'success') {
        return; // Already filled this round — no more trades for this asset
      }
      if (this._executedThisRound.get(roundKey) === 'pending') {
        return; // Trade in progress — wait for result
      }

      const RETRY_COOLDOWN_MS = 5_000;
      if (this._lastFailTs[symbol] && (Date.now() - this._lastFailTs[symbol]) < RETRY_COOLDOWN_MS) {
        return;
      }

      // Lock as "pending" to prevent concurrent dispatches
      this._executedThisRound.set(roundKey, 'pending');
      this.dispatcher.execute(bestOpp).then((trade) => {
        if (trade && trade.success) {
          // Lock permanently — 1 successful trade per round per asset
          this._executedThisRound.set(roundKey, 'success');
          console.log(`[Monitor] ${symbol}: trade SUCCESS — locked for this round`);
        } else if (trade && trade.partial) {
          this._executedThisRound.set(roundKey, 'success');
          console.error(`[Monitor] ${symbol}: PARTIAL FILL — LOCKED to prevent double exposure`);
        } else if (trade?.reason?.startsWith('poly depth') || trade?.reason?.startsWith('kalshi matchable')) {
          this._executedThisRound.delete(roundKey);
          this._lastFailTs[symbol] = Date.now();
          const skipSide = trade.reason.startsWith('poly') ? 'P' : 'K';
          console.log(`[Monitor] ${symbol}: SKIP ${skipSide} (${trade.reason})`);
        } else {
          this._executedThisRound.delete(roundKey);
          this._lastFailTs[symbol] = Date.now();
          console.log(`[Monitor] ${symbol}: FAILED — retry in 5s`);
        }
        if (trade) this.emit('trade', trade);
      }).catch((e) => {
        // Error — unlock for retry after cooldown
        this._executedThisRound.delete(roundKey);
        this._lastFailTs[symbol] = Date.now();
        console.error(`[Monitor] dispatch error:`, e.message);
      });
    }
  }

  unlockAssetForRound(symbol) {
    const roundKey = `${symbol}-${this._currentWindowTs}`;
    if (this._executedThisRound.has(roundKey)) {
      this._executedThisRound.delete(roundKey);
      console.log(`[Monitor] ${symbol}: UNLOCKED after early exit — re-entry enabled for this round`);
    }
  }
}

module.exports = Monitor;
