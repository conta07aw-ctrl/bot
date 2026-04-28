/**
 * Decision Engine — detects cross-platform arbitrage opportunities.
 *
 * Arbitrage logic:
 *   Kalshi and Polymarket both offer binary 15-min crypto markets (Up/Down).
 *   If the combined cost of opposing sides across platforms < $1.00,
 *   buying both guarantees a profit (one side always wins → pays $1.00).
 *
 * Two possible legs:
 *   Leg A: Buy Kalshi YES (Up) + Polymarket DOWN → profit if total < $1.00
 *   Leg B: Buy Kalshi NO (Down) + Polymarket UP  → profit if total < $1.00
 *
 * ROI = (1.00 - totalCost - fees) / totalCost
 */

const config = require('../config');

class DecisionEngine {
  constructor(capitalGuard) {
    this.capitalGuard = capitalGuard;
    this.kalshiFee = config.fees.kalshi;
    this.polyFee = config.fees.polymarket;
    this.minRoiPct = config.fees.minRoiPct;
    this.maxStrikeDiff = 5.0; // max $ difference between platform strikes
    this.signals = [];
    this.maxSignals = 100;
  }

  /**
   * Throttled log — max once per 30s per key to avoid WS tick spam.
   */
  _logThrottled(key, msg) {
    if (!this._logTimers) this._logTimers = {};
    const now = Date.now();
    if (this._logTimers[key] && now - this._logTimers[key] < 30_000) return;
    this._logTimers[key] = now;
    // Evict expired entries every 50 writes to prevent unbounded growth
    if (Object.keys(this._logTimers).length > 200) {
      for (const k of Object.keys(this._logTimers)) {
        if (now - this._logTimers[k] > 60_000) delete this._logTimers[k];
      }
    }
    console.log(msg);
  }

  /**
   * Update engine params from user settings.
   */
  configure({ minRoiPct, maxStrikeDiff }) {
    if (minRoiPct != null) this.minRoiPct = minRoiPct;
    if (maxStrikeDiff != null) this.maxStrikeDiff = maxStrikeDiff;
  }

  /**
   * Scan prices for a single asset and detect arbitrage opportunities.
   *
   * @param {string} assetSymbol - e.g. "BTC"
   * @param {Array} kalshiMarkets - normalized Kalshi markets
   * @param {Array} polyMarkets - normalized Polymarket markets
   * @returns {Array} opportunities found
   */
  scan(assetSymbol, kalshiMarkets, polyMarkets, overrides = {}) {
    // Apply per-asset overrides (from user filters) temporarily
    const savedMinRoi = this.minRoiPct;
    const savedMaxStrike = this.maxStrikeDiff;
    const savedEntrySize = this.capitalGuard ? this.capitalGuard.entrySize : null;
    if (overrides.minRoiPct != null) this.minRoiPct = overrides.minRoiPct;
    if (overrides.maxStrikeDiff != null) this.maxStrikeDiff = overrides.maxStrikeDiff;
    if (overrides.entrySize != null && this.capitalGuard) this.capitalGuard.entrySize = overrides.entrySize;

    const opportunities = [];

    if (!kalshiMarkets.length || !polyMarkets.length) {
      this.minRoiPct = savedMinRoi;
      this.maxStrikeDiff = savedMaxStrike;
      if (savedEntrySize != null && this.capitalGuard) this.capitalGuard.entrySize = savedEntrySize;
      return opportunities;
    }

    // Only use the NEAREST (most current) Polymarket market per asset
    // to avoid matching against expired/stale markets from other windows
    const now = Date.now();
    const activePolyMarkets = polyMarkets.filter((pm) => {
      if (pm.closed) return false;
      if (!pm.active) return false;
      // endDate from Gamma API is date-only (e.g. "2026-04-03") without time.
      // Parsing with new Date() gives midnight UTC, which is BEFORE the actual expiry.
      // Use end-of-day (23:59:59 UTC) to avoid false "expired" for today's markets.
      if (pm.endDate) {
        const endStr = pm.endDate.length <= 10 ? `${pm.endDate}T23:59:59Z` : pm.endDate;
        if (new Date(endStr).getTime() < now) {
          return false;
        }
      }
      return true;
    });

    // Sort by most recent (use slug timestamp or endDate) and take only the latest
    if (activePolyMarkets.length > 1) {
      activePolyMarkets.sort((a, b) => {
        const tsA = a.endDate ? new Date(a.endDate).getTime() : 0;
        const tsB = b.endDate ? new Date(b.endDate).getTime() : 0;
        return tsA - tsB; // earliest expiry first = current window
      });
    }
    // Only use the first (nearest expiry = current window)
    const currentPolyMarket = activePolyMarkets.length > 0 ? [activePolyMarkets[0]] : [];

    // Filter out settled Polymarket markets using REAL best ask from the
    // WS-maintained book. Don't trust pm.yesPrice/pm.noPrice from Gamma —
    // those are last/mid prices and were the root of phantom signals.
    const tradablePolyMarkets = currentPolyMarket.filter((pm) => {
      const yA = pm.yesAsk || 0;
      const nA = pm.noAsk || 0;
      // Either side missing = WS book not populated yet for that token.
      // Don't confuse this with "settled" — the YES token may have received
      // its first book event while the NO token hasn't yet (or vice versa).
      if (yA <= 0 || nA <= 0) return false;
      if (yA <= 0.10 || yA >= 0.90 || nA <= 0.10 || nA >= 0.90) return false;
      return true;
    });

    if (tradablePolyMarkets.length === 0 || kalshiMarkets.length === 0) {
      this.minRoiPct = savedMinRoi;
      this.maxStrikeDiff = savedMaxStrike;
      if (savedEntrySize != null && this.capitalGuard) this.capitalGuard.entrySize = savedEntrySize;
      return [];
    }

    for (const km of kalshiMarkets) {
      // Skip markets with no prices yet (recently created, no orderbook).
      // yesAsk=0 means no data, NOT settled.
      const yAsk = km.yesAsk || 0;
      const nAsk = km.noAsk || 0;
      if (yAsk === 0 && nAsk === 0) continue;
      if ((yAsk > 0 && (yAsk >= 0.90 || yAsk <= 0.10)) || (nAsk > 0 && (nAsk >= 0.90 || nAsk <= 0.10))) continue;

      for (const pm of tradablePolyMarkets) {

        // Use ONLY the WS-maintained best ask. yesAsk/noAsk are populated by
        // polymarket._handleWsMessage from book + price_change events.
        // Don't fall back to yesPrice/noPrice — those come from Gamma's
        // last/mid price and were the source of phantom signals.
        const polyDownAsk = pm.noAsk || 0;
        const polyUpAsk = pm.yesAsk || 0;

        // --- Leg A: Kalshi YES + Polymarket DOWN ---
        const legA = this._evaluateLeg({
          asset: assetSymbol,
          legName: 'A',
          kalshiSide: 'yes',
          polySide: 'down',
          kalshiPrice: this._getKalshiBuyPrice(km, 'yes'),
          polyPrice: polyDownAsk,
          kalshiMarket: km,
          polyMarket: pm,
        });
        if (legA) opportunities.push(legA);

        // --- Leg B: Kalshi NO + Polymarket UP ---
        const legB = this._evaluateLeg({
          asset: assetSymbol,
          legName: 'B',
          kalshiSide: 'no',
          polySide: 'up',
          kalshiPrice: this._getKalshiBuyPrice(km, 'no'),
          polyPrice: polyUpAsk,
          kalshiMarket: km,
          polyMarket: pm,
        });
        if (legB) opportunities.push(legB);
      }
    }

    // Restore engine-level defaults
    this.minRoiPct = savedMinRoi;
    this.maxStrikeDiff = savedMaxStrike;
    if (savedEntrySize != null && this.capitalGuard) this.capitalGuard.entrySize = savedEntrySize;

    return opportunities;
  }

  /**
   * Evaluate a single arbitrage leg.
   */
  _evaluateLeg({ asset, legName, kalshiSide, polySide, kalshiPrice, polyPrice, kalshiMarket, polyMarket }) {
    // Need valid prices on both sides
    if (!kalshiPrice || kalshiPrice <= 0 || !polyPrice || polyPrice <= 0) return null;

    // Polymarket tokenId MUST exist — without it, order placement will fail
    const polyTokenId = polySide === 'up' ? polyMarket.yesTokenId : polyMarket.noTokenId;
    if (!polyTokenId || polyTokenId.trim() === '') {
      return null;
    }

    // Both strikes MUST be present — if either is null, block the trade entirely.
    // Without confirmed strikes from both platforms, we cannot verify the markets
    // are aligned and risk executing on mismatched rounds.
    const kalshiStrike = kalshiMarket.strike;
    const polyStrike = polyMarket.strike;
    if (kalshiStrike == null || polyStrike == null) return null;

    // Max strike difference filter (in dollars)
    const strikeDiff = Math.abs(kalshiStrike - polyStrike);
    if (strikeDiff > this.maxStrikeDiff) return null;

    const totalCostRaw = kalshiPrice + polyPrice;

    // If combined cost >= $1.00, no arbitrage exists
    if (totalCostRaw >= 1.0) return null;

    // Calculate fees on the winning side ($1.00 payout)
    const kalshiProfit = 1.0 - kalshiPrice;
    const polyProfit = 1.0 - polyPrice;

    // Worst-case fee scenario (conservative)
    const feeIfKalshiWins = kalshiProfit * this.kalshiFee;
    const feeIfPolyWins = polyProfit * this.polyFee;
    const maxFee = Math.max(feeIfKalshiWins, feeIfPolyWins);

    // Net profit per unit after fees
    const grossSpread = 1.0 - totalCostRaw;
    const netProfit = grossSpread - maxFee;

    if (netProfit <= 0) return null;

    // ROI as percentage
    const roi = netProfit / totalCostRaw;
    const roiPct = roi * 100;

    if (roiPct < this.minRoiPct) return null;

    // Position sizing via capital guard (fixed entry size)
    const sizing = this.capitalGuard ? this.capitalGuard.size(totalCostRaw) : null;

    const signal = {
      id: `${asset}-${legName}-${Date.now()}`,
      asset,
      leg: legName,
      timestamp: new Date().toISOString(),

      // Kalshi side
      kalshiSide,
      kalshiTicker: kalshiMarket.ticker,
      kalshiPrice: Math.round(kalshiPrice * 100) / 100,
      kalshiEvent: kalshiMarket.eventTicker,

      // Polymarket side
      polySide,
      polyConditionId: polyMarket.conditionId,
      polyTokenId,
      polyPrice: Math.round(polyPrice * 1000) / 1000,
      polySlug: polyMarket.slug,

      // Financials
      totalCost: Math.round(totalCostRaw * 1000) / 1000,
      grossSpread: Math.round(grossSpread * 1000) / 1000,
      estimatedFee: Math.round(maxFee * 1000) / 1000,
      netProfit: Math.round(netProfit * 1000) / 1000,
      roiPct: Math.round(roiPct * 100) / 100,

      // Sizing (null if bankroll too small or ROI too low)
      sizing,
      execute: sizing !== null,

      // Guards propagated to dispatcher — so live-price re-validation uses the
      // same thresholds the signal was generated with.
      minRoiPct: this.minRoiPct,
      entrySize: this.capitalGuard ? this.capitalGuard.entrySize : null,
      kalshiFee: this.kalshiFee,
      polyFee: this.polyFee,
    };

    // Store signal
    this.signals.push(signal);
    if (this.signals.length > this.maxSignals) {
      this.signals = this.signals.slice(-this.maxSignals);
    }

    const action = signal.execute ? 'EXECUTE' : 'WATCH';
    this._logThrottled(`${asset}-${legName}-${action}`,
      `[Decision] ${action} ${asset} Leg${legName}: ` +
      `Kalshi ${kalshiSide}@${kalshiPrice.toFixed(3)} + Poly ${polySide}@${polyPrice.toFixed(3)} = ` +
      `${totalCostRaw.toFixed(3)} | ROI: ${roiPct.toFixed(2)}% | ` +
      `Net: $${netProfit.toFixed(3)}${sizing ? ` | Size: ${sizing.units} units ($${sizing.betSize})` : ''}`
    );

    return signal;
  }

  /**
   * Get the buy price for a Kalshi market side.
   * Uses the ask price (what we'd pay to buy).
   * Falls back to lastPrice if no ask available.
   */
  _getKalshiBuyPrice(market, side) {
    if (side === 'yes') {
      return market.yesAsk > 0 ? market.yesAsk : market.lastPrice || 0;
    }
    return market.noAsk > 0 ? market.noAsk : 0;
  }

  /**
   * Get recent signals for API exposure.
   */
  getSignals() {
    return this.signals;
  }

  /**
   * Get only actionable (execute=true) signals.
   */
  getActiveSignals() {
    return this.signals.filter((s) => s.execute);
  }
}

module.exports = DecisionEngine;
