/**
 * Parallel dispatcher — fires orders on both platforms simultaneously.
 *
 * Strategy: "True Parallel"
 *   - Pre-validate the arb against live top-of-book prices
 *   - Fire Poly FOK and Kalshi limit simultaneously via Promise.allSettled
 *   - Handle all four outcome quadrants (OK/OK, OK/FAIL, FAIL/OK, FAIL/FAIL)
 *   - Emergency-hedge the filled leg whenever the other one doesn't fill
 *
 * Rationale for going fully parallel: the prior sequential flow made Kalshi
 * wait ~1-1.5s for Poly to return, by which time Kalshi's book had usually
 * moved and the limit order rested unfilled (the "Type C" failure mode).
 * Firing both at once keeps the two legs on the same microsecond-old book.
 *
 * Safety:
 *   - TRADE_MODE must be 'live' to execute real orders
 *   - In 'dry-run' mode (default), orders are logged but not sent
 *   - EMERGENCY HEDGE: if only one leg fills, immediately closes it
 *   - Balance check: won't trade if platform balance below stop threshold
 */

const config = require('../config');

// Max age (ms) for a WS cache snapshot before it's treated as stale and the
// dispatcher falls back to REST. 5s is conservative — Kalshi/Poly book ticks
// land every 100-500ms in active hours, so anything older than 5s suggests
// the WS stream is unhealthy for that specific market.
const WS_LIVECHECK_MAX_AGE_MS = 5_000;

class Dispatcher {
  constructor(kalshi, polymarket, capitalGuard) {
    this.kalshi = kalshi;
    this.polymarket = polymarket;
    this.capitalGuard = capitalGuard;
    this.tradeMode = config.tradeMode;
    this.trades = [];
    this.maxTrades = 500;
    this._executingAssets = new Map();
    this.hedgeLog = []; // emergency hedge history
    this._openPositions = []; // filled arb positions awaiting early exit or expiry
    this._earlyExitListening = false;
    this._earlyExitTickHandler = null;
    this._checkingExits = false;

    // Global toggle: when true, step 1 reads top-of-book from the in-memory
    // WS cache (~20ms). When false, falls back to REST (~150-1800ms).
    // Hot-set by server.js PUT /api/system/livecheck.
    this.useWsLivecheck = true;

    // Max WS snapshot age (ms) allowed for LIVE trades. If either side's
    // WS snapshot is older than this, the trade is skipped — stale data
    // causes resting orders and partial fills. Dry-run uses the normal
    // 5s threshold (WS_LIVECHECK_MAX_AGE_MS) for monitoring purposes.
    // Hot-set via server.js PUT /api/system/ws-age or user settings.
    this.maxWsAgeForLive = 300;
  }

  /**
   * Step 1 helper — fetch live orderbooks for both legs.
   *
   * Two paths:
   *   1. WS fast-path (default): read in-memory mini-books from each connector.
   *      Both must exist AND be fresh (<5s) AND have non-empty top-of-book.
   *      Returns immediately, no network roundtrip.
   *   2. REST fallback: parallel Promise.all on both REST orderbook endpoints.
   *      Used when fast-path is disabled OR when cache is stale/empty.
   *
   * Returns { polyBook, kalshiBook, source, polyAgeMs, kalshiAgeMs }
   *   source is 'ws' | 'rest'.
   *   polyAgeMs/kalshiAgeMs are the WS snapshot ages (null for REST).
   * Either book may be null on REST failure — caller already handles that.
   */
  async _fetchLiveBooks(signal) {
    // ---- Path 1: WS fast-path ----
    if (this.useWsLivecheck) {
      const polyWs = this.polymarket.getLiveSnapshot(signal.polyTokenId);
      const kalshiWs = this.kalshi.getLiveSnapshot(signal.kalshiTicker);

      // WS fast-path: accept the cached book if (a) it has data and (b) the
      // WS connection is alive (received pong within 20s). We no longer reject
      // based on book age alone — a quiet book with no updates in 2s is still
      // the correct price if the WS is healthy.
      const polyOk = polyWs
        && polyWs.bestAsk != null
        && polyWs.wsAlive;
      const kalshiOk = kalshiWs
        && (kalshiWs.yes.length > 0 || kalshiWs.no.length > 0)
        && kalshiWs.wsAlive;

      if (polyOk && kalshiOk) {
        const polyBook = { asks: polyWs.asks, bids: polyWs.bids };
        const kalshiBook = { yes: kalshiWs.yes, no: kalshiWs.no };
        return {
          polyBook, kalshiBook, source: 'ws',
          polyAgeMs: polyWs.ageMs, kalshiAgeMs: kalshiWs.ageMs,
          polyWsAlive: true, kalshiWsAlive: true,
        };
      }

      // Cache miss — log why and fall through to REST.
      const reasons = [];
      if (!polyWs) reasons.push('poly:no-book');
      else if (polyWs.bestAsk == null) reasons.push('poly:no-ask');
      else if (!polyWs.wsAlive) reasons.push('poly:ws-dead');
      if (!kalshiWs) reasons.push('kalshi:no-book');
      else if (kalshiWs.yes.length === 0 && kalshiWs.no.length === 0) reasons.push('kalshi:empty');
      else if (!kalshiWs.wsAlive) reasons.push('kalshi:ws-dead');
      console.log(`[Dispatcher] step 1: WS fast-path miss (${reasons.join(', ')}) — falling back to REST`);
    }

    // ---- Path 2: REST fallback ----
    const [polyBook, kalshiBook] = await Promise.all([
      this.polymarket.getOrderbook(signal.polyTokenId).catch(e => { console.error(`[Dispatcher] Poly book FETCH FAILED: ${e.message}`); return null; }),
      this.kalshi.getOrderbook(signal.kalshiTicker).catch(e => { console.error(`[Dispatcher] Kalshi book FETCH FAILED: ${e.message}`); return null; }),
    ]);
    return { polyBook, kalshiBook, source: 'rest', polyAgeMs: null, kalshiAgeMs: null };
  }

  /**
   * Execute an arbitrage signal on both platforms in parallel.
   * Includes emergency hedge if only one leg fills.
   */
  async execute(signal) {
    const asset = signal.asset;
    if (this._executingAssets.get(asset)) {
      console.log(`[Dispatcher] skipping — already executing ${asset}`);
      return { success: false, reason: `busy:${asset}` };
    }

    this._executingAssets.set(asset, true);

    try {
      const isDryRun = this.tradeMode !== 'live';
      const prefix = isDryRun ? '[DRY-RUN]' : '[LIVE]';

      // Check capital guard before executing.
      // SYNCHRONOUS — reads cached balances only (no network I/O).
      // The cache is kept warm by a background timer in capitalGuard.start()
      // and refreshed again after each trade via _recordTrade below.
      if (this.capitalGuard && !isDryRun) {
        const allowed = this.capitalGuard.canTrade();
        if (!allowed.ok) {
          console.log(`[Dispatcher] BLOCKED by capital guard: ${allowed.reason}`);
          return this._recordTrade(signal, { success: false, reason: allowed.reason, blocked: true });
        }
      }

      const units = signal.sizing?.units || 0;

      if (isDryRun) {
        return this._recordTrade(signal, {
          success: true,
          dryRun: true,
          kalshiResult: { simulated: true },
          polyResult: { simulated: true },
        });
      }

      // --- LIVE EXECUTION ---
      if (units <= 0) {
        return this._recordTrade(signal, { success: false, reason: 'no units to trade' });
      }

      // Pre-flight: validate Polymarket can actually execute
      if (!signal.polyTokenId || signal.polyTokenId.trim() === '') {
        console.error(`[Dispatcher] ABORT — Polymarket tokenId is empty for ${signal.asset} Leg${signal.leg}`);
        return this._recordTrade(signal, { success: false, blocked: true, reason: 'polyTokenId empty' });
      }
      if (!this.polymarket.wallet) {
        console.error(`[Dispatcher] ABORT — Polymarket wallet not configured (missing POLY_PRIVATE_KEY)`);
        return this._recordTrade(signal, { success: false, blocked: true, reason: 'poly wallet missing' });
      }
      if (!this.polymarket.apiKey) {
        console.error(`[Dispatcher] ABORT — Polymarket API key not configured`);
        return this._recordTrade(signal, { success: false, blocked: true, reason: 'poly apiKey missing' });
      }

      // ============================================================
      //  LIVE PRICE EXECUTION:
      //  1. Fetch LIVE orderbooks from both platforms
      //  2. Validate arb still exists with real ask prices
      //  3. Place Poly FOK at real ask → Kalshi at real ask
      // ============================================================

      // Step 1: Fetch LIVE orderbooks from BOTH platforms.
      // Two paths inside _fetchLiveBooks:
      //   - WS fast-path (when this.useWsLivecheck=true): in-memory cache,
      //     ~20ms total, gated by 5s freshness threshold.
      //   - REST fallback: parallel Promise.all on both platforms.
      // CRITICAL: If either orderbook fails to fetch, ABORT the trade entirely.
      // Never trade on stale signal prices — proxy 522 errors cause bad fills.
      let polyLiveAsk = null;
      let polyLiveQty = 0;
      let kalshiLivePrice = null;
      let kalshiLiveQty = 0;

      const { polyBook, kalshiBook, source: bookSource, polyAgeMs, kalshiAgeMs, polyWsAlive, kalshiWsAlive } = await this._fetchLiveBooks(signal);

      // GATE: WS health — reject only if the WebSocket connection is dead.
      // Previously this used a 300ms book-age threshold, but that killed 80%
      // of valid opportunities because quiet markets don't send book updates
      // every 300ms. The real question is: "is the WS alive?" (confirmed via
      // ping/pong). If it is, cached prices are trustworthy — no update means
      // no price change. The _validateLive() check below still re-validates
      // ROI with these prices before any money is spent.
      if (bookSource === 'ws') {
        const dead = [];
        if (polyWsAlive === false) dead.push(`poly(age=${polyAgeMs}ms)`);
        if (kalshiWsAlive === false) dead.push(`kalshi(age=${kalshiAgeMs}ms)`);
        if (dead.length > 0) {
          console.log(`[Dispatcher] SKIP — WS connection dead: ${dead.join(', ')} — waiting for reconnect`);
          return this._recordTrade(signal, { success: false, reason: `ws dead: ${dead.join(', ')}` });
        }
      }

      // GATE: Both orderbooks MUST succeed — abort if either failed
      if (!polyBook || !kalshiBook) {
        const failed = [];
        if (!polyBook) failed.push('Polymarket');
        if (!kalshiBook) failed.push('Kalshi');
        console.error(`[Dispatcher] ABORT — orderbook fetch failed for: ${failed.join(', ')} — refusing to trade on stale prices`);
        return this._recordTrade(signal, { success: false, reason: `orderbook fetch failed: ${failed.join(', ')}` });
      }

      // Parse Poly orderbook — get best ask (lowest price someone is selling at)
      const polyAsks = polyBook.asks || [];
      if (polyAsks.length > 0) {
        const sorted = [...polyAsks]
          .map(a => ({ price: parseFloat(a.price || a[0] || 0), size: parseFloat(a.size || a[1] || 0) }))
          .filter(a => a.price > 0 && a.size > 0)
          .sort((a, b) => a.price - b.price);
        if (sorted.length > 0) {
          polyLiveAsk = sorted[0].price;
          polyLiveQty = sorted[0].size;
        }
      }

      if (polyLiveAsk === null) {
        console.error(`[Dispatcher] ABORT — Poly orderbook returned but has no valid asks — refusing to trade`);
        return this._recordTrade(signal, { success: false, reason: 'poly orderbook empty (no asks)' });
      }

      // Get Kalshi ask price from the cached market object (populated by the
      // ticker WS channel and initial REST discovery). This is the authoritative
      // best ask — NOT derived from the orderbook, which contains mixed
      // bids+asks and produces wrong prices via the binary complement method.
      const wantSide = signal.kalshiSide; // 'yes' or 'no'
      const oppositeSide = wantSide === 'yes' ? 'no' : 'yes';
      const kCached = this.kalshi.markets.get(signal.kalshiTicker);
      if (kCached) {
        kalshiLivePrice = wantSide === 'yes' ? (kCached.yesAsk || null) : (kCached.noAsk || null);
      }

      // Calculate MATCHABLE depth: only count opposite-side levels whose
      // price can actually fill our limit order.
      //
      // How Kalshi matching works in a binary market:
      //   BUY NO at 55¢ → matches YES orders at ≥ 45¢ (because 45+55 = 100)
      //   BUY YES at 35¢ → matches NO orders at ≥ 65¢ (because 35+65 = 100)
      //
      // The old code summed ALL opposite-side levels regardless of price,
      // reporting "623 depth" when the real matchable volume at our price
      // was 0. This caused trades to fire into empty books → IOC reject →
      // emergency hedge → loss.
      if (kalshiLivePrice === null) {
        console.error(`[Dispatcher] ABORT — Kalshi cached ask is null for ${signal.kalshiTicker}`);
        return this._recordTrade(signal, { success: false, reason: 'kalshi ask not available' });
      }

      const oppLevels = kalshiBook ? kalshiBook[oppositeSide] : null;
      let kalshiTotalDepth = 0;  // total across all levels (for logging)
      if (oppLevels && oppLevels.length > 0) {
        const limitCents = Math.round(kalshiLivePrice * 100);
        const matchThreshold = (100 - limitCents) / 100; // min price for match
        for (const l of oppLevels) {
          const qty = Number(l.qty) || 0;
          kalshiTotalDepth += qty;
          if (l.price >= matchThreshold) {
            kalshiLiveQty += qty;
          }
        }
      }

      // Step 2: Validate arb with LIVE prices BEFORE spending any money.
      // Applies ROI floor and entrySize cap using the SAME thresholds the
      // decisionEngine used at signal time — fixes slippage bypass bugs.
      // Poly depth gate: SKIP if the best ask doesn't have enough size
      // for our full order. Same rationale as the Kalshi gate below —
      // partial fills create qty mismatches that get hedged at a loss.
      let adjustedUnits = units;
      const MIN_CONTRACTS = 3;

      // Dynamic entry size with asymmetric depth buffers.
      // Poly FAK POST takes ~350ms — uses a larger buffer to absorb book
      // movement between Kalshi fill and Poly order arrival.
      // Kalshi IOC is faster (~100ms) — buffer of 0 is enough.
      const POLY_DEPTH_BUFFER = 3;
      const KALSHI_DEPTH_BUFFER = 0;
      const maxByDepth = Math.min(
        Math.floor(polyLiveQty) - POLY_DEPTH_BUFFER,
        Math.floor(kalshiLiveQty) - KALSHI_DEPTH_BUFFER,
      );

      if (maxByDepth < MIN_CONTRACTS) {
        return this._recordTrade(signal, {
          success: false,
          reason: `depth insuficiente: min(poly=${polyLiveQty.toFixed(1)}-${POLY_DEPTH_BUFFER}, kalshi=${kalshiLiveQty.toFixed(1)}-${KALSHI_DEPTH_BUFFER})=${maxByDepth} < ${MIN_CONTRACTS}`,
        });
      }

      if (maxByDepth < adjustedUnits) {
        console.log(`[Dispatcher] dynamic entry: ${adjustedUnits}→${maxByDepth} (poly=${polyLiveQty.toFixed(1)}-${POLY_DEPTH_BUFFER}, kalshi=${kalshiLiveQty.toFixed(1)}-${KALSHI_DEPTH_BUFFER})`);
        adjustedUnits = maxByDepth;
      }

      // FIX #3: Update expectedProfit if units was reduced by depth
      if (adjustedUnits < units) {
        signal.expectedProfit = signal.netProfit * adjustedUnits;
      }

      const preCheck = this._validateLive({
        signal,
        polyPrice: polyLiveAsk,
        kalshiPrice: kalshiLivePrice,
        maxUnits: adjustedUnits,
      });
      if (!preCheck.ok) {
        console.warn(`[Dispatcher] ABORT pre-poly — ${preCheck.reason} | poly=$${polyLiveAsk} kalshi=$${kalshiLivePrice}`);

        // DIAG: dump WS mini-book vs REST side-by-side when pre-check aborts.
        // This shows exactly which level is phantom in the WS cache. Read-only,
        // no mutation. REMOVE once parser is fixed.
        try {
          const pmCached = this.polymarket.markets.get(signal.polyConditionId);
          const relevantTokenId = signal.polySide === 'up'
            ? pmCached?.yesTokenId
            : pmCached?.noTokenId;
          const wsBook = this.polymarket._books.get(relevantTokenId);
          if (wsBook) {
            const askDump = [...wsBook.asks.entries()]
              .sort((a, b) => parseFloat(a[0]) - parseFloat(b[0]))
              .slice(0, 5)
              .map(([p, s]) => `${p}:${s}`)
              .join(' ');
            const bidDump = [...wsBook.bids.entries()]
              .sort((a, b) => parseFloat(b[0]) - parseFloat(a[0]))
              .slice(0, 5)
              .map(([p, s]) => `${p}:${s}`)
              .join(' ');
            const restAskDump = (polyBook.asks || [])
              .map(a => ({ price: parseFloat(a.price), size: parseFloat(a.size) }))
              .filter(a => a.price > 0 && a.size > 0)
              .sort((a, b) => a.price - b.price)
              .slice(0, 5)
              .map(a => `${a.price}:${a.size}`)
              .join(' ');
            const restBidDump = (polyBook.bids || [])
              .map(a => ({ price: parseFloat(a.price), size: parseFloat(a.size) }))
              .filter(a => a.price > 0 && a.size > 0)
              .sort((a, b) => b.price - a.price)
              .slice(0, 5)
              .map(a => `${a.price}:${a.size}`)
              .join(' ');
            console.log(`[DiagBook] ${signal.polySide} token=${(relevantTokenId || '').slice(0, 12)}...`);
            console.log(`[DiagBook]   WS asks: ${askDump || '(empty)'}`);
            console.log(`[DiagBook]   WS bids: ${bidDump || '(empty)'}`);
            console.log(`[DiagBook] REST asks: ${restAskDump || '(empty)'}`);
            console.log(`[DiagBook] REST bids: ${restBidDump || '(empty)'}`);
          } else {
            console.log(`[DiagBook] NO WS BOOK for token ${(relevantTokenId || '').slice(0, 12)}...`);
          }
        } catch (diagErr) {
          console.warn(`[DiagBook] failed: ${diagErr.message}`);
        }

        return this._recordTrade(signal, {
          success: false,
          partial: false,
          hedged: false,
          kalshiResult: `skipped (${preCheck.reason})`,
          polyResult: 'skipped',
        });
      }
      if (preCheck.units < adjustedUnits) {
        console.log(`[Dispatcher] adjusted units (entrySize cap): ${adjustedUnits} → ${preCheck.units} (live cost $${preCheck.totalCostPerUnit.toFixed(4)})`);
        adjustedUnits = preCheck.units;
      }
      // Cap units to ensure we can hedge (sell back) if Kalshi fails.
      // Poly SELL requires $1.00 PUSD per contract as collateral.
      // After buying N contracts at polyPrice, remaining = balance - N*polyPrice
      // To sell N back: need N * $1.00. So: balance - N*price >= N → N <= balance/(price+1)
      //
      // Uses the CACHED balance (refreshed in background every 60s and after
      // each trade). We accept up to 60s of staleness here because:
      //   - entrySize is small (~$10), so one stale trade costs bounded $
      //   - the alternative (sync RPC fetch here) puts 500-2800ms of network
      //     I/O on the critical path, which is what killed the last 3 live
      //     trades (signal aged out before execution).
      if (this.capitalGuard) {
        const polyBalance = this.capitalGuard.getCachedBalances().polymarket || 0;
        if (polyBalance > 0) {
          const maxHedgeable = Math.floor(polyBalance / (polyLiveAsk + 1.0));
          if (maxHedgeable < adjustedUnits) {
            if (maxHedgeable < 1) {
              console.warn(`[Dispatcher] balance too low to hedge: $${polyBalance.toFixed(2)} — need $${(polyLiveAsk + 1).toFixed(2)}/contract — skipping`);
              return this._recordTrade(signal, { success: false, reason: 'balance too low for hedge' });
            }
            console.log(`[Dispatcher] capped units ${adjustedUnits} → ${maxHedgeable} (hedge safety: $${polyBalance.toFixed(2)} cached balance)`);
            adjustedUnits = maxHedgeable;
          }
        }
      }

      // Final gate: Polymarket rejects orders with notional < $1.00 PUSD.
      // When notional is below the minimum, try to upsize units to meet it.
      // The entry size is a preference, not a safety limit — the real caps
      // are hedge safety and platform balance.
      const notional = adjustedUnits * polyLiveAsk;
      if (notional < 1.01 && polyLiveAsk > 0) {
        const minUnits = Math.ceil(1.01 / polyLiveAsk);
        // Check if upsized units still fit within hedge cap and balance
        let canUpsize = true;
        if (this.capitalGuard) {
          const polyBalance = this.capitalGuard.getCachedBalances().polymarket || 0;
          if (polyBalance > 0) {
            const maxHedgeable = Math.floor(polyBalance / (polyLiveAsk + 1.0));
            if (minUnits > maxHedgeable) canUpsize = false;
          }
        }
        if (canUpsize && minUnits >= 1) {
          console.log(`[Dispatcher] notional upsize: ${adjustedUnits} → ${minUnits} to meet Poly $1 min (${minUnits} × $${polyLiveAsk} = $${(minUnits * polyLiveAsk).toFixed(2)})`);
          adjustedUnits = minUnits;
        } else {
          console.warn(
            `[Dispatcher] notional $${notional.toFixed(2)} below Poly $1 min `
            + `(${adjustedUnits} × $${polyLiveAsk}) — upsize to ${minUnits} blocked by hedge cap — skipping`,
          );
          return this._recordTrade(signal, {
            success: false,
            reason: `notional $${notional.toFixed(2)} < $1 min order size`,
          });
        }
      }

      console.log(`${prefix} [Dispatcher] EXEC ${signal.asset} Leg${signal.leg}: K:${signal.kalshiSide}@${kalshiLivePrice} P:${signal.polySide}@${polyLiveAsk} x${adjustedUnits} ROI:${signal.roiPct}%`);

      // Step 3: Parallel execution — fire both legs simultaneously.
      //
      // Both orders fire at t=0 via Promise.allSettled. Total latency =
      // max(Kalshi ~80ms, Poly ~350ms) instead of sum (~430ms sequential).
      // The Poly book is hit 80ms sooner, before it can move.
      //
      // Four outcome quadrants:
      //   Both OK        → success (handle qty mismatch if partial fills)
      //   Kalshi OK only → hedge Kalshi (sell back via IOC)
      //   Poly OK only   → hedge Poly (sell back via FAK)
      //   Both FAIL      → $0 at risk, nothing to hedge
      // Dynamic price improvement: use the ROI buffer above minRoiPct to
      // improve our limit prices and increase fill probability. Instead of a
      // fixed ¢ improvement, we calculate the max combined cost that still
      // meets the user's minimum ROI, then split the surplus as improvement.
      const rawCombined = kalshiLivePrice + polyLiveAsk;
      const kalshiFee = signal.kalshiFee ?? 0;
      const polyFee = signal.polyFee ?? 0;
      const worstFee = Math.max((1 - kalshiLivePrice) * kalshiFee, (1 - polyLiveAsk) * polyFee);
      const maxCombined = (1.00 - worstFee) / (1 + (signal.minRoiPct ?? 3) / 100);
      const dynamicImprovement = Math.max(0, maxCombined - rawCombined);
      // Round UP so sub-cent improvement always becomes at least 1¢ effective.
      // Math.round was killing 0.3-0.8¢ improvements → FAK at exact ask → 0 fills.
      const polyOrderPrice = Math.ceil((polyLiveAsk + dynamicImprovement) * 100) / 100;
      const kalshiOrderPrice = kalshiLivePrice;

      // Validate the ceil-rounded price still meets the ROI floor.
      // If rounding up pushed the effective combined above budget, skip — the
      // signal doesn't have enough ROI margin for reliable fills.
      const effectiveCombined = kalshiOrderPrice + polyOrderPrice;
      const effectiveWorstFee = Math.max(
        (1 - kalshiOrderPrice) * kalshiFee,
        (1 - polyOrderPrice) * polyFee,
      );
      const effectiveNet = 1.0 - effectiveCombined - effectiveWorstFee;
      const effectiveRoi = effectiveCombined > 0 ? (effectiveNet / effectiveCombined) * 100 : 0;
      if (effectiveNet <= 0 || effectiveRoi < (signal.minRoiPct ?? 3) * 0.5) {
        console.log(`[Dispatcher] SKIP — improvement exceeds ROI budget: effective=${effectiveCombined.toFixed(3)} roi=${effectiveRoi.toFixed(1)}% (need ≥${((signal.minRoiPct ?? 3) * 0.5).toFixed(1)}%)`);
        return this._recordTrade(signal, {
          success: false,
          reason: `improvement exceeds ROI budget (${effectiveRoi.toFixed(1)}% < ${((signal.minRoiPct ?? 3) * 0.5).toFixed(1)}%)`,
        });
      }

      console.log(`[Dispatcher] dynamic improvement: ${(dynamicImprovement * 100).toFixed(1)}¢ → FAK@${polyOrderPrice} (effective roi=${effectiveRoi.toFixed(1)}%, raw=$${rawCombined.toFixed(3)} max=$${maxCombined.toFixed(3)})`);

      const kalshiOrder = {
        ticker: signal.kalshiTicker,
        side: signal.kalshiSide,
        action: 'buy',
        count: adjustedUnits,
        price: kalshiOrderPrice,
      };

      const polyOrder = {
        tokenId: signal.polyTokenId,
        price: polyOrderPrice,
        size: adjustedUnits,
        side: 'BUY',
        orderType: 'FAK',
      };

      console.log(`[Dispatcher] PARALLEL: Kalshi IOC BUY ${wantSide}@${kalshiOrderPrice} x${adjustedUnits} + Poly FAK BUY @${polyOrderPrice} x${adjustedUnits}`);
      const t0 = Date.now();

      const [kalshiSettled, polySettled] = await Promise.allSettled([
        this.kalshi.placeOrder(kalshiOrder),
        this.polymarket.placeOrder(polyOrder),
      ]);
      const totalMs = Date.now() - t0;

      // --- Parse Kalshi result ---
      let kalshiResult = kalshiSettled.status === 'fulfilled' ? kalshiSettled.value : null;
      if (kalshiSettled.status === 'rejected') {
        console.error(`[Dispatcher] Kalshi exception: ${kalshiSettled.reason?.message}`);
      }
      const kalshiOk = kalshiResult !== null;
      const kalshiFilled = kalshiResult?._actualFilled || (kalshiOk ? adjustedUnits : 0);

      if (kalshiResult?._phantomFill) {
        console.warn(`[Dispatcher] KALSHI PHANTOM FILL: ${kalshiFilled} contracts filled after cancel-not-found`);
      }

      // --- Parse Poly result ---
      let polyResult = polySettled.status === 'fulfilled' ? polySettled.value : null;
      if (polySettled.status === 'rejected') {
        console.error(`[Dispatcher] Poly exception: ${polySettled.reason?.message}`);
      }
      const polyOk = polyResult !== null;

      const polyFilledQty = polyOk
        ? this._extractPolyFilledQty(polyResult, 'BUY', polyOrderPrice, adjustedUnits)
        : 0;

      console.log(
        `[Dispatcher] PARALLEL settled in ${totalMs}ms — `
        + `Kalshi: ${kalshiOk ? `OK ${kalshiFilled}x@$${kalshiOrderPrice}` : 'FAIL'}, `
        + `Poly: ${polyOk ? `OK ${polyFilledQty}x@$${polyOrderPrice}` : 'FAIL'}`
      );

      // --- Quadrant handling ---
      const matchedQty = Math.min(kalshiFilled, polyFilledQty);
      let hedgeResult = null;

      if (!kalshiOk && kalshiFilled === 0 && !polyOk && polyFilledQty === 0) {
        // Both FAIL — $0 at risk
        console.log(`[Dispatcher] both legs failed — $0 at risk`);
        return this._recordTrade(signal, {
          success: false,
          partial: false,
          hedged: false,
          kalshiResult: 'failed',
          polyResult: 'failed',
          totalMs,
        });
      }

      if (kalshiFilled > 0 && polyFilledQty === 0) {
        // Kalshi OK, Poly FAIL → hedge Kalshi
        console.warn(`[Dispatcher] Kalshi filled ${kalshiFilled}x, Poly 0 — hedging Kalshi`);
        hedgeResult = await this._emergencyHedge(signal, kalshiFilled, false, true);
        return this._recordTrade(signal, {
          success: false,
          partial: true,
          hedged: !!hedgeResult,
          hedgeResult,
          kalshiResult: kalshiOk ? kalshiResult : 'failed',
          polyResult: polyOk ? polyResult : 'failed',
          polyFilledQty: 0,
          kalshiFilled,
          kalshiPrice: kalshiOrderPrice,
          polyPrice: polyOrderPrice,
          totalCost: 0,
          totalMs,
        });
      }

      if (polyFilledQty > 0 && kalshiFilled === 0) {
        // Poly OK, Kalshi FAIL → hedge Poly
        console.warn(`[Dispatcher] Poly filled ${polyFilledQty}x, Kalshi 0 — hedging Poly`);
        hedgeResult = await this._emergencyHedge(signal, polyFilledQty, true, false);
        return this._recordTrade(signal, {
          success: false,
          partial: true,
          hedged: !!hedgeResult,
          hedgeResult,
          kalshiResult: kalshiOk ? kalshiResult : 'failed',
          polyResult: polyOk ? polyResult : 'failed',
          polyFilledQty,
          kalshiFilled: 0,
          kalshiPrice: kalshiOrderPrice,
          polyPrice: polyOrderPrice,
          totalCost: 0,
          totalMs,
        });
      }

      // Both filled — handle qty mismatch
      if (kalshiFilled > polyFilledQty) {
        const excess = kalshiFilled - polyFilledQty;
        console.warn(`[Dispatcher] QTY MISMATCH — Kalshi: ${kalshiFilled}, Poly: ${polyFilledQty} — hedging ${excess} excess on Kalshi`);
        hedgeResult = await this._emergencyHedge(signal, excess, false, true);
      } else if (polyFilledQty > kalshiFilled) {
        const excess = polyFilledQty - kalshiFilled;
        console.warn(`[Dispatcher] QTY MISMATCH — Poly: ${polyFilledQty}, Kalshi: ${kalshiFilled} — hedging ${excess} excess on Poly`);
        hedgeResult = await this._emergencyHedge(signal, excess, true, false);
      }

      const success = matchedQty > 0 && kalshiFilled === polyFilledQty;

      const result = this._recordTrade(signal, {
        success,
        partial: kalshiFilled !== polyFilledQty,
        hedged: !!hedgeResult,
        hedgeResult,
        kalshiResult: kalshiOk ? kalshiResult : 'failed',
        polyResult: polyOk ? polyResult : 'failed',
        polyFilledQty,
        kalshiFilled,
        kalshiPrice: kalshiOrderPrice,
        polyPrice: polyOrderPrice,
        totalCost: (polyOrderPrice + kalshiOrderPrice) * matchedQty,
        totalMs,
      });

      // Track matched contracts for early exit monitoring
      if (matchedQty > 0) {
        const entryRoiPct = signal.roiPct ?? signal.minRoiPct ?? 3;
        const polyFilledQtySafe = Math.max(0, Number(polyFilledQty || 0));
        this._openPositions.push({
          id: result.id,
          asset: signal.asset,
          leg: signal.leg,
          qty: matchedQty,
          kalshiTicker: signal.kalshiTicker,
          kalshiSide: signal.kalshiSide,
          kalshiPrice: kalshiOrderPrice,
          polyTokenId: signal.polyTokenId,
          polySide: signal.polySide,
          polyPrice: polyOrderPrice,
          entryCost: kalshiOrderPrice + polyOrderPrice,
          kalshiFee: signal.kalshiFee ?? 0,
          polyFee: signal.polyFee ?? 0,
          polyFilledQty: polyFilledQtySafe,
          entryRoiPct,
          entryTime: Date.now(),
          readyAt: Date.now() + 15_000,
        });
        console.log(`[EarlyExit] tracking position: ${signal.asset} Leg${signal.leg} x${matchedQty} polyTokens=${polyFilledQtySafe.toFixed(2)} cost=$${(kalshiOrderPrice + polyOrderPrice).toFixed(3)} minExitROI=${entryRoiPct.toFixed(2)}% (ready in 15s)`);
      }

      return result;
    } finally {
      this._executingAssets.delete(asset);
    }
  }

  /**
   * Return the top-N sell-side price levels for an emergency hedge SELL on
   * Polymarket, sorted best-first (highest bid first). Reads the WS cache
   * first (same Map that powers the fast-path), falls back to REST, and
   * returns an empty array if both fail.
   *
   * The hedge walks down this list when a level can't absorb the size. Each
   * entry is `{ price: number, size: number, source: 'ws'|'rest' }`.
   */
  async _getPolyHedgeBids(tokenId) {
    // Try WS cache
    try {
      const snap = this.polymarket.getLiveSnapshot(tokenId);
      if (snap && Array.isArray(snap.bids) && snap.bids.length > 0) {
        return snap.bids
          .filter((l) => l.price > 0 && l.size > 0)
          .map((l) => ({ price: l.price, size: l.size, source: 'ws' }));
      }
    } catch (_) { /* fall through */ }

    // REST fallback
    try {
      const book = await this.polymarket.getOrderbook(tokenId);
      if (book && Array.isArray(book.bids) && book.bids.length > 0) {
        return book.bids
          .map((l) => ({ price: parseFloat(l.price), size: parseFloat(l.size) }))
          .filter((l) => l.price > 0 && l.size > 0)
          .sort((a, b) => b.price - a.price)
          .slice(0, 5)
          .map((l) => ({ ...l, source: 'rest' }));
      }
    } catch (_) { /* fall through */ }

    return [];
  }

  /**
   * Same as _getPolyHedgeBids but for Kalshi. The side to sell (yes|no) maps
   * directly to the bid ladder on that side — SELL YES means lifting someone
   * else's YES bid. Falls back to REST on cache miss.
   */
  async _getKalshiHedgeBids(ticker, side) {
    // Try WS cache
    try {
      const snap = this.kalshi.getLiveSnapshot(ticker);
      if (snap) {
        const levels = side === 'yes' ? snap.yes : snap.no;
        if (Array.isArray(levels) && levels.length > 0) {
          return levels
            .filter((l) => l.price > 0 && l.qty > 0)
            .map((l) => ({ price: l.price, size: l.qty, source: 'ws' }));
        }
      }
    } catch (_) { /* fall through */ }

    // REST fallback
    try {
      const book = await this.kalshi.getOrderbook(ticker);
      if (book) {
        const levels = side === 'yes' ? book.yes : book.no;
        if (Array.isArray(levels) && levels.length > 0) {
          return levels
            .filter((l) => l.price > 0 && (l.qty ?? l.size ?? 0) > 0)
            .map((l) => ({ price: l.price, size: l.qty ?? l.size, source: 'rest' }))
            .sort((a, b) => b.price - a.price)
            .slice(0, 5);
        }
      }
    } catch (_) { /* fall through */ }

    return [];
  }

  /**
   * EMERGENCY HEDGE: immediately close the filled leg when the other fails.
   *
   * If Polymarket filled but Kalshi failed → SELL on Polymarket
   * If Kalshi filled but Polymarket failed → SELL on Kalshi
   */
  async _emergencyHedge(signal, units, polyFilled, kalshiFilled) {
    const hedge = {
      timestamp: new Date().toISOString(),
      asset: signal.asset,
      leg: signal.leg,
      polyFilled,
      kalshiFilled,
      hedgeAttempts: [],
      estimatedSellPrice: 0,
    };

    // Constants shared by both hedge branches
    const FLOOR_PRICE = 0.01;       // absolute fallback if the book is dead
    const MAX_ROUNDS = 15;          // overall safety cap (90s worst-case)
    const SETTLE_WAIT_BASE_MS = 2000;  // initial settlement wait (Polygon ~2s blocks)
    const SETTLE_WAIT_MAX_MS = 8000;   // ramp up wait on later rounds

    try {
      if (polyFilled && !kalshiFilled) {
        // Polymarket filled, Kalshi failed → sell on Polymarket.
        //
        // Strategy: FAK (Fill and Kill) market sell at floor price ($0.01).
        // A SELL at $0.01 matches ALL bids ≥ $0.01, sweeping the book.
        // FAK fills whatever is available and kills the rest — no waiting
        // for the entire order to fill. This eliminates the settlement
        // delay problem: if 3 of 5 tokens have settled, we sell 3 immediately.
        // Retry loop handles remaining tokens as they settle on-chain.
        console.warn('[HEDGE] Selling Polymarket position via FAK market sell');
        let hedgedQty = 0;
        let dustExit = false;

        let round = 0;
        while (hedgedQty < units && round < MAX_ROUNDS) {
          round++;
          const remaining = units - hedgedQty;

          // Wait for settlement between rounds (not on round 1 — fire immediately).
          if (round > 1) {
            const waitMs = Math.min(SETTLE_WAIT_BASE_MS + (round - 2) * 1000, SETTLE_WAIT_MAX_MS);
            console.log(`[HEDGE] waiting ${(waitMs / 1000).toFixed(1)}s for settlement (round ${round})...`);
            await new Promise(r => setTimeout(r, waitMs));
          }

          // Check CLOB-reported token balance from previous round's error.
          const lastErr = this.polymarket._lastOrderError;
          const clobTokens = lastErr && lastErr.type === 'balance' && lastErr.polyBalance != null
            ? lastErr.polyBalance
            : null;

          // Size the sell: use CLOB balance if known (fractional OK), otherwise
          // try full remaining. Floor to 2 decimals — CLOB rejects higher precision.
          let sellSize = remaining;
          if (clobTokens != null) {
            const exactTokens = Math.floor(clobTokens * 100) / 100;
            if (exactTokens < 0.01 && hedgedQty > 0) {
              console.warn(
                `[HEDGE] remaining balance ${exactTokens} tok ≈ 0 — `
                + `fee dust. hedged=${hedgedQty}/${units}. Exiting.`,
              );
              dustExit = true;
              break;
            }
            sellSize = Math.min(remaining, exactTokens);
          }

          if (sellSize <= 0) {
            if (hedgedQty === 0 && round % 3 === 0) {
              console.log(`[HEDGE] sell cap = 0 but 0 hedged — probing FAK SELL (round ${round})`);
              sellSize = remaining;
            } else {
              console.log('[HEDGE] sell cap = 0 — waiting for settlement...');
              continue;
            }
          }

          // Log book top for diagnostics (no-cost WS read)
          const bids = await this._getPolyHedgeBids(signal.polyTokenId);
          if (bids.length > 0) {
            const top = bids[0];
            console.log(`[HEDGE] Poly book top (${top.source}): $${top.price.toFixed(3)} x${top.size} (${bids.length} levels)`);
            if (round === 1) hedge.estimatedSellPrice = top.price;
          } else {
            console.warn('[HEDGE] Poly book empty — FAK will sweep at floor');
          }

          // FAK market sell: price=$0.01 sweeps all bids. Whatever is available
          // (settled tokens + book liquidity) fills instantly, rest is killed.
          console.log(`[HEDGE] Poly FAK SELL: ${sellSize}x @$${FLOOR_PRICE} (hedged: ${hedgedQty}/${units})`);
          const result = await this.polymarket.placeOrder({
            tokenId: signal.polyTokenId,
            price: FLOOR_PRICE,
            size: sellSize,
            side: 'SELL',
            orderType: 'FAK',
          });
          const ok = result !== null;
          hedge.hedgeAttempts.push({
            platform: 'polymarket',
            action: `FAK-SELL-${sellSize}x@${FLOOR_PRICE}`,
            success: ok,
            result,
          });

          if (ok) {
            // FAK success — may be partial fill. Assume full sellSize filled
            // (CLOB accepted the order). If it was partial, next round's
            // balance check will reveal what's left.
            hedgedQty += sellSize;
            console.log(`[HEDGE] Poly FAK SELL success: ${sellSize}x (total hedged: ${hedgedQty}/${units})`);
          } else {
            const err = this.polymarket._lastOrderError;
            if (err && err.type === 'balance') {
              console.log(
                `[HEDGE] balance reject — CLOB shows `
                + `${err.polyBalance != null ? err.polyBalance.toFixed(4) : '?'} tok available`,
              );
            } else if (err && err.type === 'liquidity') {
              console.log('[HEDGE] FAK rejected — no liquidity at all');
            } else {
              console.log(`[HEDGE] FAK failed — ${err?.type || 'unknown'} error`);
            }
          }
        }

        if (hedgedQty >= units) {
          console.log(`[HEDGE] SUCCESS — closed all ${hedgedQty} contracts`);
        } else if (dustExit) {
          console.warn(
            `[HEDGE] DUST EXIT — closed ${hedgedQty}/${units}. `
            + `${units - hedgedQty} contract(s) left as fee-skim dust (auto-settles on resolution).`,
          );
        } else if (hedgedQty > 0) {
          console.warn(`[HEDGE] PARTIAL — closed ${hedgedQty}/${units}. ${units - hedgedQty} still exposed.`);
        } else {
          console.error('[HEDGE] CRITICAL — 0 contracts closed. FULL EXPOSURE!');
        }
        if (hedgedQty < units && !dustExit) {
          console.error(`[HEDGE] Manual action needed: sell ${units - hedgedQty}x ${signal.polyTokenId.slice(0, 16)}...`);
        }
      }

      if (kalshiFilled && !polyFilled) {
        // Kalshi filled, Polymarket failed → sell on Kalshi.
        //
        // Strategy: IOC sweep at floor price ($0.01), same approach as Poly hedge.
        // SELL YES at $0.01 = "accept any price >= $0.01" → Kalshi matches against
        // the highest YES bid first, then next, etc. IOC cancels unfilled remainder.
        //
        // Why NOT walk the book level by level (old approach):
        //   Kalshi's orderbook (yes_dollars/no_dollars) contains MIXED bids+asks.
        //   The highest level can be an ask, not a bid. Selling at ask prices
        //   produces 0 fills (RESTING) every time.
        console.warn('[HEDGE] Selling Kalshi position via IOC floor sweep');
        let hedgedQty = 0;

        let round = 0;
        while (hedgedQty < units && round < MAX_ROUNDS) {
          round++;
          const remaining = units - hedgedQty;

          if (round > 1) {
            const waitMs = Math.min(1000 + (round - 2) * 500, 3000);
            console.log(`[HEDGE] waiting ${(waitMs / 1000).toFixed(1)}s before retry (round ${round})...`);
            await new Promise(r => setTimeout(r, waitMs));
          }

          // Log book top for diagnostics (read-only, no pricing decisions)
          const bids = await this._getKalshiHedgeBids(signal.kalshiTicker, signal.kalshiSide);
          if (bids.length > 0) {
            const top = bids[0];
            console.log(`[HEDGE] Kalshi book top (${top.source}): $${top.price.toFixed(2)} x${top.size} (${bids.length} levels)`);
            if (round === 1) hedge.estimatedSellPrice = top.price;
          } else {
            console.warn('[HEDGE] Kalshi book appears empty — IOC will try anyway');
          }

          console.log(`[HEDGE] Kalshi IOC SELL: ${remaining}x @$${FLOOR_PRICE} (hedged: ${hedgedQty}/${units})`);
          const result = await this.kalshi.placeOrder({
            ticker: signal.kalshiTicker,
            side: signal.kalshiSide,
            action: 'sell',
            count: remaining,
            price: FLOOR_PRICE,
          });
          const ok = result !== null;
          const actualFilled = result?._actualFilled ?? (ok ? remaining : 0);
          hedge.hedgeAttempts.push({
            platform: 'kalshi',
            action: `IOC-SELL-${remaining}x@${FLOOR_PRICE}`,
            success: ok && actualFilled > 0,
            result,
          });

          if (ok && actualFilled > 0) {
            hedgedQty += actualFilled;
            console.log(`[HEDGE] Kalshi IOC SELL success: ${actualFilled}x (total hedged: ${hedgedQty}/${units})`);
          } else {
            console.log(`[HEDGE] Kalshi IOC SELL: 0 fills this round`);
          }
        }

        if (hedgedQty >= units) {
          console.log(`[HEDGE] SUCCESS — closed all ${hedgedQty} Kalshi contracts`);
        } else if (hedgedQty > 0) {
          console.warn(`[HEDGE] PARTIAL — closed ${hedgedQty}/${units} Kalshi. ${units - hedgedQty} still exposed.`);
        } else {
          console.error('[HEDGE] CRITICAL — Kalshi sell FAILED. OPEN EXPOSURE!');
        }
        if (hedgedQty < units) {
          console.error(`[HEDGE] Manual action needed: sell ${units - hedgedQty}x ${signal.kalshiSide} on ${signal.kalshiTicker}`);
        }
      }
    } catch (err) {
      console.error('[HEDGE] emergency hedge error:', err.message);
      hedge.error = err.message;
    }

    // Log the hedge
    this.hedgeLog.push(hedge);
    if (this.hedgeLog.length > 100) this.hedgeLog = this.hedgeLog.slice(-100);

    const allOk = hedge.hedgeAttempts.every((a) => a.success);
    console.log(`[HEDGE] ${allOk ? 'SUCCESS' : 'FAILED'} — ${hedge.hedgeAttempts.length} attempt(s)`);

    return hedge;
  }

  _recordTrade(signal, result) {
    const trade = {
      id: `trade-${Date.now()}`,
      signalId: signal.id,
      asset: signal.asset,
      leg: signal.leg,
      timestamp: new Date().toISOString(),
      kalshiSide: signal.kalshiSide,
      kalshiPrice: signal.kalshiPrice,
      kalshiTicker: signal.kalshiTicker,
      polySide: signal.polySide,
      polyPrice: signal.polyPrice,
      kalshiFee: signal.kalshiFee ?? 0,
      polyFee: signal.polyFee ?? 0,
      units: signal.sizing?.units || 0,
      totalCost: signal.totalCost,
      expectedRoi: signal.roiPct,
      expectedProfit: signal.netProfit * (signal.sizing?.units || 0),
      ...result,
    };

    // --- P&L calculation ---
    let pnl = 0;
    if (!result.dryRun && !result.blocked) {
      const polyQty    = result.polyFilledQty  || 0;
      const kalshiQty  = result.kalshiFilled   || 0;
      const matchedQty = Math.min(polyQty, kalshiQty);
      const hedgeSellPrice = result.hedgeResult?.estimatedSellPrice || 0;

      // Worst-case fee on matched arb contracts (same formula as decisionEngine)
      const kalshiFeeRate = signal.kalshiFee ?? 0;
      const polyFeeRate   = signal.polyFee ?? 0;
      const worstFee = matchedQty > 0
        ? Math.max(
            (1.0 - (result.kalshiPrice || 0)) * kalshiFeeRate,
            (1.0 - (result.polyPrice || 0))   * polyFeeRate,
          )
        : 0;

      if (matchedQty > 0) {
        pnl += (1.00 - (result.polyPrice || 0) - (result.kalshiPrice || 0) - worstFee) * matchedQty;
      }

      // Loss on Kalshi excess hedged back (Kalshi filled > Poly filled)
      // Includes Kalshi exchange fee on the buy side: 7% of (1 - buyPrice).
      // This fee is charged upfront when placing the buy order and is NOT
      // refunded when selling back — it's a sunk cost on every hedge.
      if (result.hedged && kalshiQty > polyQty) {
        const excessQty = kalshiQty - polyQty;
        const buyPrice = result.kalshiPrice || 0;
        const kalshiBuyFee = (1 - buyPrice) * kalshiFeeRate;
        pnl -= ((buyPrice - hedgeSellPrice) + kalshiBuyFee) * excessQty;
      }

      // Loss on Poly excess hedged back (Poly filled > Kalshi filled)
      // Includes Poly token fee: bought N contracts but received N*(1-fee)
      // tokens, so we sell fewer tokens back → additional loss.
      if (result.hedged && polyQty > kalshiQty) {
        const excessQty = polyQty - kalshiQty;
        const buyPrice = result.polyPrice || 0;
        const polyTokenFee = buyPrice * polyFeeRate;
        pnl -= ((buyPrice - hedgeSellPrice) + polyTokenFee) * excessQty;
      }
    }
    pnl = Math.round(pnl * 100) / 100;
    trade.pnl = pnl;

    if (this.capitalGuard && !result.dryRun && !result.blocked) {
      this.capitalGuard.recordTrade({ pnl });
    }

    this.trades.push(trade);
    if (this.trades.length > this.maxTrades) {
      this.trades = this.trades.slice(-this.maxTrades);
    }

    // Fire-and-forget post-trade balance refresh. Keeps the cached balance
    // fresh for the next round's hedge-cap check without blocking this call.
    // Errors are swallowed — the background timer will catch up on the next
    // tick. Only runs for real trades (skip dry-run and blocked).
    if (this.capitalGuard && !result.dryRun && !result.blocked) {
      this.capitalGuard.refreshBalances().catch(() => {});
    }

    const status = result.dryRun ? 'DRY-RUN' :
                   result.blocked ? 'BLOCKED' :
                   result.hedged ? 'HEDGED' :
                   result.success ? 'FILLED' :
                   result.partial ? 'PARTIAL' : 'FAILED';

    // --- C3: single-line detailed trade log ---
    const polyTag = result.polyFilledQty
      ? `P:$${(result.polyPrice || 0).toFixed(2)}/${result.polyFilledQty}`
      : (result.polyResult === 'failed' ? 'P:FAIL' : 'P:--');
    const kalshiTag = result.kalshiFilled
      ? `K:$${(result.kalshiPrice || 0).toFixed(2)}/${result.kalshiFilled}`
      : (result.kalshiResult === 'not_fired' ? 'K:not_fired' : 'K:FAIL');
    const costTag = result.success
      ? (() => {
        const kP = result.kalshiPrice || 0;
        const pP = result.polyPrice || 0;
        const combined = kP + pP;
        const kFee = signal.kalshiFee ?? 0;
        const pFee = signal.polyFee ?? 0;
        const worstFee = Math.max((1 - kP) * kFee, (1 - pP) * pFee);
        const net = 1 - combined - worstFee;
        const roi = combined > 0 ? (net / combined * 100).toFixed(1) : '0.0';
        return ` cost=$${combined.toFixed(2)} roi=${roi}%`;
      })()
      : '';
    const hedgeSell = result.hedgeResult?.estimatedSellPrice || 0;
    const hedgeTag = result.hedged
      ? ` hedge:${result.hedgeResult?.hedgeAttempts?.length || 0}att@$${hedgeSell.toFixed(2)}`
      : '';
    const msTag = result.totalMs ? ` ${result.totalMs}ms` : '';
    const cumPnl = this.capitalGuard ? this.capitalGuard.getState().totalPnl : 0;
    const reasonTag = result.reason ? ` reason="${result.reason}"` : '';

    console.log(`[TRADE] ${status} ${signal.asset} Leg${signal.leg} x${trade.units} | ${polyTag} ${kalshiTag}${costTag}${hedgeTag} |${msTag} pnl=$${pnl.toFixed(2)} cumPnl=$${cumPnl.toFixed(2)}${reasonTag}`);

    return trade;
  }

  /**
   * Infer filled base-contract quantity from a Polymarket order response.
   *
   * The CLOB response may expose different fields by route/side
   * (makingAmount, takingAmount, sizeMatched). We normalize to contracts and
   * clamp by requested size to avoid over-counting.
   */
  _extractPolyFilledQty(polyResult, side, price, requestedSize) {
    if (!polyResult) return 0;

    const req = Math.max(0, Number(requestedSize || 0));
    const p = Number(price || 0);
    const makingAmt = Number(polyResult.makingAmount || 0);
    const takingAmt = Number(polyResult.takingAmount || 0);
    const sizeMatched = Number(polyResult.sizeMatched || 0);
    const candidates = [];

    if (sizeMatched > 0) candidates.push(sizeMatched);

    if (side === 'BUY') {
      if (takingAmt > 0) candidates.push(takingAmt);
      if (makingAmt > 0 && p > 0) candidates.push(makingAmt / p);
    } else {
      if (makingAmt > 0) candidates.push(makingAmt);
      if (takingAmt > 0 && p > 0) candidates.push(takingAmt / p);
    }

    const positives = candidates.filter(v => Number.isFinite(v) && v > 0);
    if (positives.length === 0) return 0;

    const inferred = Math.min(...positives);
    const clamped = req > 0 ? Math.min(inferred, req) : inferred;
    return Math.max(0, Math.floor(clamped * 10_000) / 10_000);
  }

  /**
   * Re-validate an arbitrage with LIVE prices, applying the same ROI /
   * entrySize guards the decisionEngine used at signal time.
   *
   * Fixes two slippage bugs:
   *   1. Dispatcher used to only check `combined < 1.00`, ignoring minRoiPct
   *      → let through trades with post-slippage ROI far below the user's floor.
   *   2. Dispatcher used to size once at signal time → the entrySize cap could be
   *      violated if prices moved against us between signal and execution.
   *
   * Returns { ok, reason?, units, roiPct, netProfit, totalCostPerUnit, betSize }.
   * Caller must honor the returned `units` — it may be lower than the original.
   */
  _validateLive({ signal, polyPrice, kalshiPrice, maxUnits }) {
    if (!polyPrice || polyPrice <= 0 || !kalshiPrice || kalshiPrice <= 0) {
      return { ok: false, reason: 'invalid prices' };
    }

    const combined = polyPrice + kalshiPrice;
    if (combined >= 1.0) {
      return { ok: false, reason: `no arb (combined=$${combined.toFixed(4)})`, roiPct: 0, units: 0 };
    }

    // Worst-case fees — same formula as decisionEngine
    const kalshiFee = signal.kalshiFee ?? 0;
    const polyFee = signal.polyFee ?? 0;
    const kalshiProfit = 1.0 - kalshiPrice;
    const polyProfit = 1.0 - polyPrice;
    const maxFee = Math.max(kalshiProfit * kalshiFee, polyProfit * polyFee);

    const grossSpread = 1.0 - combined;
    const netProfit = grossSpread - maxFee;
    if (netProfit <= 0) {
      return { ok: false, reason: `fees kill (gross=${grossSpread.toFixed(4)} fees=${maxFee.toFixed(4)})`, roiPct: 0, units: 0 };
    }

    const roiPct = (netProfit / combined) * 100;
    const minRoi = signal.minRoiPct ?? 0;
    if (roiPct < minRoi) {
      return {
        ok: false,
        reason: `ROI=${roiPct.toFixed(2)}% < min=${minRoi}%`,
        roiPct,
        netProfit,
        units: 0,
      };
    }

    // Re-cap units against entrySize using the LIVE per-unit cost.
    // This enforces the same fixed-$ entry limit the user configured.
    const entrySize = signal.entrySize ?? null;
    let units = maxUnits;
    if (entrySize != null && entrySize > 0) {
      const maxUnitsByEntrySize = Math.floor(entrySize / combined);
      if (maxUnitsByEntrySize < units) units = maxUnitsByEntrySize;
    }
    if (units < 1) {
      return {
        ok: false,
        reason: `entrySize cap (entry=$${entrySize} combined=$${combined.toFixed(4)} → ${units} units)`,
        roiPct,
        netProfit,
        units: 0,
      };
    }

    const betSize = Math.round(units * combined * 100) / 100;
    return { ok: true, units, roiPct, netProfit, totalCostPerUnit: combined, betSize };
  }

  getTrades() { return this.trades; }
  getHedgeLog() { return this.hedgeLog; }
  getOpenPositions() { return this._openPositions; }

  /**
   * Start the early exit monitor. Hooks into the same WS 'tick' events
   * that the Monitor uses for arb detection — fires on EVERY price update
   * from Kalshi and Polymarket, zero polling delay.
   */
  startEarlyExitMonitor() {
    if (this._earlyExitListening) return;
    this._earlyExitTickHandler = () => {
      if (this._openPositions.length === 0) return;
      this._checkEarlyExits().catch(err => {
        console.error(`[EarlyExit] tick error: ${err.message}`);
        this._checkingExits = false;
      });
    };
    this.kalshi.on('tick', this._earlyExitTickHandler);
    this.polymarket.on('tick', this._earlyExitTickHandler);
    this._earlyExitListening = true;
    console.log(`[EarlyExit] monitor started — event-driven (every WS tick)`);
  }

  stopEarlyExitMonitor() {
    if (this._earlyExitTickHandler) {
      this.kalshi.removeListener('tick', this._earlyExitTickHandler);
      this.polymarket.removeListener('tick', this._earlyExitTickHandler);
      this._earlyExitTickHandler = null;
    }
    this._earlyExitListening = false;
  }

  /**
   * Clear positions for a specific asset (called on round rotation).
   * Positions that weren't early-exited will settle naturally at expiry.
   */
  clearPositionsForAsset(asset) {
    const before = this._openPositions.length;
    this._openPositions = this._openPositions.filter(p => p.asset !== asset);
    const removed = before - this._openPositions.length;
    if (removed > 0) {
      console.log(`[EarlyExit] cleared ${removed} expired position(s) for ${asset}`);
    }
  }

  /**
   * Synchronous check of all open positions against live WS bids.
   * If a position can be closed at a profit, fires the async exit.
   * Skips sides already closed (kalshiClosed / polyClosed flags).
   */
  async _checkEarlyExits() {
    if (this._openPositions.length === 0) return;
    if (this.tradeMode !== 'live') return;
    if (this._checkingExits) return;
    this._checkingExits = true;

    try {
      for (const pos of this._openPositions) {
        if (pos._exiting) continue;

        // Wait for Polygon settlement before attempting any sell
        if (pos.readyAt && Date.now() < pos.readyAt) continue;

        // If only one side remains, delegate to emergency hedge instead of
        // the early exit loop — the hedge has settlement retry + dust detection.
        if (pos.kalshiClosed && !pos.polyClosed) {
          pos._exiting = true;
          console.log(`[EarlyExit] Poly-only remainder — delegating to emergency hedge`);
          this._emergencyHedge(
            { polyTokenId: pos.polyTokenId, polySide: pos.polySide, kalshiTicker: pos.kalshiTicker, kalshiSide: pos.kalshiSide },
            pos.qty, true, false,
          ).then(() => {
            this._openPositions = this._openPositions.filter(p => p.id !== pos.id);
          }).catch(err => {
            console.error(`[EarlyExit] hedge error:`, err.message);
            this._openPositions = this._openPositions.filter(p => p.id !== pos.id);
          });
          continue;
        }
        if (pos.polyClosed && !pos.kalshiClosed) {
          pos._exiting = true;
          console.log(`[EarlyExit] Kalshi-only remainder — delegating to emergency hedge`);
          this._emergencyHedge(
            { polyTokenId: pos.polyTokenId, polySide: pos.polySide, kalshiTicker: pos.kalshiTicker, kalshiSide: pos.kalshiSide },
            pos.qty, false, true,
          ).then(() => {
            this._openPositions = this._openPositions.filter(p => p.id !== pos.id);
          }).catch(err => {
            console.error(`[EarlyExit] hedge error:`, err.message);
            this._openPositions = this._openPositions.filter(p => p.id !== pos.id);
          });
          continue;
        }

        // Both sides still open — first check if Poly balance has settled.
        // Floor to 2 decimals: the Poly CLOB rejects quantities with more
        // than 2 decimal places, causing sell orders to fail silently.
        if (!pos._polyBalance) {
          try {
            const bal = await this.polymarket.getTokenBalance(pos.polyTokenId);
            if (bal != null && bal > 0) {
              pos._polyBalance = Math.floor(bal * 100) / 100;
              console.log(`[EarlyExit] ${pos.asset} Leg${pos.leg}: Poly balance settled — ${pos._polyBalance} tokens (raw=${bal})`);
            } else {
              // Fallback: after 30s of failed balance checks, estimate from actual fill
              const elapsed = Date.now() - pos.entryTime;
              if (elapsed > 30_000) {
                const rawTokens = pos.polyFilledQty || pos.qty;
                const estimatedBal = Math.floor(rawTokens * (1 - pos.polyFee) * 100) / 100;
                pos._polyBalance = estimatedBal;
                console.warn(`[EarlyExit] ${pos.asset} Leg${pos.leg}: balance API returned ${bal} after ${(elapsed / 1000).toFixed(0)}s — using estimate ${estimatedBal} (from filledQty=${rawTokens.toFixed(2)})`);
              } else {
                pos._balanceRetries = (pos._balanceRetries || 0) + 1;
                if (pos._balanceRetries % 20 === 1) {
                  console.log(`[EarlyExit] ${pos.asset} Leg${pos.leg}: balance not yet available (attempt ${pos._balanceRetries}, ${(elapsed / 1000).toFixed(0)}s elapsed)`);
                }
                continue;
              }
            }
          } catch (err) {
            console.warn(`[EarlyExit] ${pos.asset} Leg${pos.leg}: balance check error: ${err.message}`);
            continue;
          }
        }

        // --- Real-time dollar-based exit check (sync WS reads, 0ms) ---
        const kalshiCached = this.kalshi.markets.get(pos.kalshiTicker);
        const kalshiBid = pos.kalshiSide === 'yes'
          ? (kalshiCached?.yesBid || 0)
          : (kalshiCached?.noBid || 0);

        const polySnap = this.polymarket.getLiveSnapshot(pos.polyTokenId);
        const polyBid = polySnap?.bestBid || 0;

        // Need at least one side with a bid to evaluate exit
        if (kalshiBid <= 0 && polyBid <= 0) continue;

        // Net close value AFTER all exit fees (what actually hits our balance).
        // Use visible book depth to estimate revenue instead of assuming full qty at top bid.
        const kalshiSnapshot = this.kalshi.getLiveSnapshot(pos.kalshiTicker);
        const kalshiLevels = kalshiSnapshot ? (pos.kalshiSide === 'yes' ? kalshiSnapshot.yes : kalshiSnapshot.no) : [];
        const polyLevels = polySnap?.bids || [];

        const kalshiNetValue = this._estimateExitRevenue(pos.qty, kalshiLevels, pos.kalshiFee, kalshiBid);
        const polyNetValue = this._estimateExitRevenue(pos._polyBalance, polyLevels, pos.polyFee, polyBid);
        const netCloseValue = kalshiNetValue + polyNetValue;

        // Total cost INCLUDING entry fees (what we actually paid)
        const kalshiEntryCost = pos.kalshiPrice * (1 + pos.kalshiFee) * pos.qty;
        const polyEntryCost = pos.polyPrice * (1 + pos.polyFee) * pos.qty;
        const totalCost = kalshiEntryCost + polyEntryCost;

        // Net profit after ALL fees (entry + exit)
        const netProfit = netCloseValue - totalCost;

        // Periodic status log (every 30s) so user can monitor convergence
        const now = Date.now();
        if (!pos._lastStatusLog || now - pos._lastStatusLog >= 30_000) {
          pos._lastStatusLog = now;
          const grossClose = kalshiBid * pos.qty + polyBid * pos._polyBalance;
          console.log(
            `[EarlyExit] STATUS ${pos.asset} Leg${pos.leg}: `
            + `K:${kalshiBid.toFixed(2)} P:${polyBid.toFixed(2)} | `
            + `gross=$${grossClose.toFixed(2)} net=$${netCloseValue.toFixed(2)} cost=$${totalCost.toFixed(2)} `
            + `netProfit=$${netProfit.toFixed(3)}`
          );
        }

        // Only exit when net profit (after ALL fees) exceeds $0.20
        const MIN_EXIT_PROFIT = 0.20;
        if (netProfit < MIN_EXIT_PROFIT) continue;

        pos._exiting = true;
        console.log(
          `[EarlyExit] OPPORTUNITY ${pos.asset} Leg${pos.leg}: `
          + `K:${pos.kalshiSide}@${kalshiBid} net=$${kalshiNetValue.toFixed(2)} + `
          + `P:${pos.polySide}@${polyBid} net=$${polyNetValue.toFixed(2)} = `
          + `$${netCloseValue.toFixed(2)} (cost=$${totalCost.toFixed(2)}) `
          + `netProfit=$${netProfit.toFixed(3)}`
        );

        this._executeEarlyExit(pos, kalshiBid, polyBid).catch(err => {
          console.error(`[EarlyExit] error:`, err.message);
          pos._exiting = false;
        });
      }
    } finally {
      this._checkingExits = false;
    }
  }

  /**
   * Execute early exit: sell BOTH legs in PARALLEL.
   *
   * Kalshi: sells pos.qty (full integer — no token fees).
   * Poly: sells pos._polyBalance (exact fractional — fees reduce token count).
   * Both fire at once via Promise.allSettled.
   */
  async _executeEarlyExit(pos, kalshiBid, polyBid) {
    // Floor Poly qty to 2 decimals — the CLOB rejects higher precision.
    const polyQty = Math.floor((pos._polyBalance || 0) * 100) / 100;
    const kalshiQty = pos.qty;
    // Skip Poly sell when there are no bids (worthless side — let it expire).
    // Still sell the profitable Kalshi side to lock in gains.
    const skipPolySell = polyBid <= 0 || polyQty <= 0;
    console.log(
      `[EarlyExit] EXEC: ${pos.asset} Leg${pos.leg} — `
      + `K:SELL ${kalshiQty}@${kalshiBid} `
      + (skipPolySell ? `P:SKIP (bid=${polyBid} qty=${polyQty})` : `P:SELL ${polyQty}@${polyBid}`)
      + ` (parallel)`
    );
    const t0 = Date.now();

    const promises = [
      this.kalshi.placeOrder({
        ticker: pos.kalshiTicker,
        side: pos.kalshiSide,
        action: 'sell',
        count: kalshiQty,
        price: 0.01,
      }),
    ];
    if (!skipPolySell) {
      promises.push(
        this.polymarket.placeOrder({
          tokenId: pos.polyTokenId,
          price: 0.01,
          size: polyQty,
          side: 'SELL',
          orderType: 'FAK',
        }),
      );
    }

    const settled = await Promise.allSettled(promises);
    const totalMs = Date.now() - t0;

    const kalshiResult = settled[0].status === 'fulfilled' ? settled[0].value : null;
    const polyResult = !skipPolySell && settled[1]?.status === 'fulfilled' ? settled[1].value : null;
    const kalshiSold = kalshiResult?._actualFilled || (kalshiResult ? kalshiQty : 0);

    const polySold = polyResult
      ? this._extractPolyFilledQty(polyResult, 'SELL', polyBid, polyQty)
      : 0;

    // --- Revenue net of exit fees ---
    const kalshiGross = kalshiSold > 0 ? kalshiBid * kalshiSold : 0;
    const kalshiSellFee = kalshiSold > 0 ? kalshiBid * pos.kalshiFee * kalshiSold : 0;
    const polyGross = polySold > 0 ? polyBid * polySold : 0;
    const polySellFee = polySold > 0 ? polyBid * pos.polyFee * polySold : 0;
    const totalRevenue = (kalshiGross - kalshiSellFee) + (polyGross - polySellFee);

    // --- Cost including entry fees ---
    const kalshiEntryCost = pos.kalshiPrice * (1 + pos.kalshiFee) * pos.qty;
    const polyEntryCost = pos.polyPrice * (1 + pos.polyFee) * pos.qty;
    const totalCost = kalshiEntryCost + polyEntryCost;

    const pnl = Math.round((totalRevenue - totalCost) * 100) / 100;

    // When Poly sell was skipped (no bids), Kalshi-only is a SUCCESS.
    // The worthless Poly tokens expire at $0 — no action needed.
    const isSuccess = skipPolySell
      ? kalshiSold > 0
      : kalshiSold > 0 && polySold > 0;

    console.log(
      `[EarlyExit] ${isSuccess ? 'SUCCESS' : 'PARTIAL'} ${pos.asset} Leg${pos.leg} `
      + `| K:SELL ${kalshiSold}x@${kalshiBid.toFixed(3)} `
      + (skipPolySell ? `P:SKIPPED (expires $0)` : `P:SELL ${polySold}x@${polyBid.toFixed(3)}`)
      + ` | ${totalMs}ms pnl=$${pnl.toFixed(2)}`
    );

    if (this.capitalGuard && pnl !== 0) {
      this.capitalGuard.recordTrade({ pnl });
      this.capitalGuard.refreshBalances().catch(() => {});
    }

    if (isSuccess) {
      this._openPositions = this._openPositions.filter(p => p.id !== pos.id);
      if (this.monitor) {
        this.monitor.unlockAssetForRound(pos.asset);
      }
      return { kalshiSold, polySold, pnl, totalMs };
    }

    if (kalshiSold > 0 && polySold === 0 && !skipPolySell) {
      console.warn(`[EarlyExit] Kalshi sold but Poly failed — marking for hedge`);
      pos.kalshiClosed = true;
      pos._exiting = false;
    } else if (polySold > 0 && kalshiSold === 0) {
      console.warn(`[EarlyExit] Poly sold but Kalshi failed — marking for hedge`);
      pos.polyClosed = true;
      pos._exiting = false;
    } else {
      pos._exiting = false;
    }

    return { kalshiSold, polySold, pnl, totalMs };
  }

  _estimateExitRevenue(qty, levels, feeRate, fallbackPrice = 0) {
    if (!qty || qty <= 0) return 0;
    if (!Array.isArray(levels) || levels.length === 0) {
      return Math.max(0, qty * (fallbackPrice || 0) * (1 - (feeRate || 0)));
    }

    let remaining = qty;
    let gross = 0;

    for (const level of levels) {
      if (remaining <= 0) break;
      const price = Number(level.price ?? 0);
      const size = Number(level.size ?? level.qty ?? 0);
      if (!(price > 0) || !(size > 0)) continue;

      const filled = Math.min(remaining, size);
      gross += filled * price;
      remaining -= filled;
    }

    if (remaining > 0) {
      gross += remaining * (fallbackPrice || 0);
    }

    return Math.max(0, gross * (1 - (feeRate || 0)));
  }

  getState() {
    return {
      tradeMode: this.tradeMode,
      executingAssets: [...this._executingAssets.keys()],
      openPositions: this._openPositions.length,
      totalTrades: this.trades.length,
      successfulTrades: this.trades.filter((t) => t.success).length,
      partialTrades: this.trades.filter((t) => t.partial).length,
      hedgedTrades: this.trades.filter((t) => t.hedged).length,
      blockedTrades: this.trades.filter((t) => t.blocked).length,
      dryRunTrades: this.trades.filter((t) => t.dryRun).length,
    };
  }
}

module.exports = Dispatcher;
