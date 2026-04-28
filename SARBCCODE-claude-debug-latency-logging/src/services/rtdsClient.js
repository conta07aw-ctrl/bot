/**
 * RTDS Client — Real-Time Data Stream from Polymarket.
 *
 * Source: wss://ws-live-data.polymarket.com → topic: crypto_prices_chainlink
 *
 * This is the SAME Chainlink Data Streams feed that Polymarket uses internally
 * for the "Price to Beat" (strike). The first price received after a round opens
 * IS the strike — identical to what the contract uses for UP/DOWN resolution.
 *
 * Strike logic (proven pattern from working dapp):
 *   - Each 15-min round opens at ts % 900 === 0
 *   - Strike = first price from RTDS where payload.timestamp falls in [roundOpen, roundOpen+30s]
 *   - Once captured, never changes
 *   - If no tick arrives within 30s → strike stays null → trading blocked (safe)
 *
 * NO RPC fallback — Chainlink on-chain aggregator returns different prices
 * than Data Streams, causing false strike divergence.
 */

const WebSocket = require('ws');
const EventEmitter = require('events');

const RTDS_URL = 'wss://ws-live-data.polymarket.com';
const PING_INTERVAL_MS = 5_000;
const STALE_TIMEOUT_MS = 20_000;
const ROUND_SECS = 900;
const STRIKE_WINDOW_SECS = 30;

// RTDS symbol format → our asset symbols
const SYMBOL_MAP = {
  'btc/usd': 'BTC',
  'eth/usd': 'ETH',
  'sol/usd': 'SOL',
  'xrp/usd': 'XRP',
};

class RtdsClient extends EventEmitter {
  constructor() {
    super();
    this._ws = null;
    this._wsConnected = false;
    this._pingTimer = null;
    this._staleTimer = null;
    this._reconnectTimer = null;
    this._reconnectDelay = 1000;
    this._maxReconnectDelay = 30000;
    this._lastDataTime = 0;

    /**
     * Captured strikes per round.
     * Key: "{SYMBOL}-{roundOpenTimestamp}" e.g. "BTC-1753314000"
     * Value: strike price (number)
     */
    this._strikes = new Map();

    /**
     * Latest price per symbol (always updated on every tick).
     * Key: "BTC", "ETH", etc.
     * Value: { price, timestamp }
     */
    this._latestPrices = new Map();
  }

  /* ---------------------------------------------------------- */
  /*  Public API                                                 */
  /* ---------------------------------------------------------- */

  getStrike(symbol, roundOpenTs) {
    return this._strikes.get(`${symbol}-${roundOpenTs}`) ?? null;
  }

  getCurrentStrike(symbol) {
    return this.getStrike(symbol, this._getCurrentRoundTs());
  }

  getLatestPrice(symbol) {
    return this._latestPrices.get(symbol) ?? null;
  }

  getAllStrikes() {
    const result = {};
    for (const [key, value] of this._strikes) result[key] = value;
    return result;
  }

  /* ---------------------------------------------------------- */
  /*  Connection                                                 */
  /* ---------------------------------------------------------- */

  connect() {
    if (this._ws) return;
    this._connectWs();
  }

  _connectWs() {
    try {
      this._ws = new WebSocket(RTDS_URL);
    } catch (err) {
      console.error('[RTDS] failed to create connection:', err.message);
      this._scheduleReconnect();
      return;
    }

    this._ws.on('open', () => {
      console.log('[RTDS] connected to Polymarket Live Data');
      this._wsConnected = true;
      this._reconnectDelay = 1000;
      this._lastDataTime = Date.now();

      // Subscribe WITHOUT symbol filter — catch ALL symbols from the topic.
      // Per-symbol filter was only delivering BTC; other symbols may use
      // different identifiers than expected.
      this._wsSend({
        action: 'subscribe',
        subscriptions: [{
          topic: 'crypto_prices_chainlink',
          type: 'update',
          filters: '',
        }],
      });

      console.log('[RTDS] subscribed to crypto_prices_chainlink (all symbols, no filter)');
      this._startPing();
      this._startStaleCheck();
      this._startPriceLog();
    });

    this._ws.on('message', (raw) => {
      this._lastDataTime = Date.now();
      if (!raw || raw.length === 0) return;
      const text = raw.toString().trim();
      if (!text) return;
      try {
        const msg = JSON.parse(text);
        this._handleMessage(msg);
      } catch (e) {
        console.error('[RTDS] WS handler error:', e.message);
      }
    });

    this._ws.on('close', (code) => {
      console.log(`[RTDS] closed (code: ${code})`);
      this._cleanup();
      this._scheduleReconnect();
    });

    this._ws.on('error', (err) => {
      console.error('[RTDS] error:', err.message);
    });
  }

  /* ---------------------------------------------------------- */
  /*  Message handling                                           */
  /* ---------------------------------------------------------- */

  _handleMessage(msg) {
    if (!msg || msg.topic !== 'crypto_prices_chainlink') return;

    const payload = msg.payload;
    if (!payload) return;

    // Snapshot: array of historical prices (received on subscribe)
    if (Array.isArray(payload)) {
      const sorted = [...payload].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
      for (const tick of sorted) this._processTick(tick);
      return;
    }

    // Single update: { symbol, value, timestamp }
    if (payload.symbol && payload.value != null) {
      this._processTick(payload);
      return;
    }

    // Nested: { data: tick | [tick, ...] }
    if (payload.data) {
      const items = Array.isArray(payload.data) ? payload.data : [payload.data];
      for (const tick of items) this._processTick(tick);
      return;
    }
  }

  /**
   * Process a single RTDS tick.
   * This is the core strike capture logic — identical to the proven working dapp:
   *   if (secsIntoRound <= 30 && strike not yet captured) → capture it.
   */
  _processTick(tick) {
    if (!tick || !tick.symbol || tick.value == null) return;

    // Log every unique symbol we receive to discover the correct format
    if (!this._seenSymbols) this._seenSymbols = new Set();
    if (!this._seenSymbols.has(tick.symbol)) {
      this._seenSymbols.add(tick.symbol);
      console.log(`[RTDS] NEW symbol discovered: "${tick.symbol}" value=${tick.value} (total unique: ${this._seenSymbols.size})`);
    }

    const symbol = SYMBOL_MAP[tick.symbol.toLowerCase()];
    if (!symbol) return;

    const price = parseFloat(tick.value);
    if (!price || price <= 0) return;

    // Use the timestamp from the RTDS payload — this is the Chainlink timestamp
    const ts = tick.timestamp || Math.floor(Date.now() / 1000);
    const tsSecs = ts < 1e12 ? ts : Math.floor(ts / 1000);
    const tsMs = ts < 1e12 ? ts * 1000 : ts;

    // Always update latest price
    this._latestPrices.set(symbol, { price, timestamp: tsMs });

    // Strike capture: first price in the first 30s of the round
    const roundOpen = Math.floor(tsSecs / ROUND_SECS) * ROUND_SECS;
    const secsIntoRound = tsSecs - roundOpen;
    const strikeKey = `${symbol}-${roundOpen}`;

    if (secsIntoRound <= STRIKE_WINDOW_SECS && !this._strikes.has(strikeKey)) {
      this._strikes.set(strikeKey, price);
      const dec = price >= 1000 ? 2 : price >= 10 ? 4 : 5;
      console.log(`[RTDS] STRIKE captured: ${symbol} = $${price.toFixed(dec)} (round ${roundOpen}, ${secsIntoRound}s in)`);
      this.emit('strike', { symbol, price, roundOpenTs: roundOpen });
    }

    // Cleanup old strikes occasionally
    if (Math.random() < 0.01) this._cleanupOldStrikes();
  }

  /* ---------------------------------------------------------- */
  /*  Helpers                                                    */
  /* ---------------------------------------------------------- */

  _getCurrentRoundTs() {
    return Math.floor(Date.now() / 1000 / ROUND_SECS) * ROUND_SECS;
  }

  _cleanupOldStrikes() {
    const cutoff = this._getCurrentRoundTs() - (ROUND_SECS * 8);
    for (const key of this._strikes.keys()) {
      const ts = parseInt(key.split('-').pop(), 10);
      if (ts < cutoff) this._strikes.delete(key);
    }
  }

  _wsSend(obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  }

  _startPing() {
    if (this._pingTimer) clearInterval(this._pingTimer);
    this._pingTimer = setInterval(() => {
      if (this._ws && this._ws.readyState === WebSocket.OPEN) {
        this._ws.ping();
      }
    }, PING_INTERVAL_MS);
  }

  _startPriceLog() {
    if (this._priceLogTimer) clearInterval(this._priceLogTimer);
    this._priceLogTimer = setInterval(() => {
      const symbols = ['BTC', 'ETH', 'SOL', 'XRP'];
      const parts = symbols.map((s) => {
        const latest = this._latestPrices.get(s);
        if (!latest) return `${s}: no data`;
        const age = Math.round((Date.now() - latest.timestamp) / 1000);
        return `${s}: $${latest.price.toFixed(2)} (${age}s ago)`;
      });
      console.log(`[RTDS] prices: ${parts.join(', ')}`);
    }, 60_000);
  }

  _startStaleCheck() {
    if (this._staleTimer) clearInterval(this._staleTimer);
    this._staleTimer = setInterval(() => {
      if (Date.now() - this._lastDataTime > STALE_TIMEOUT_MS) {
        console.warn('[RTDS] data stream stale — forcing reconnect');
        this._forceReconnect();
      }
    }, 5_000);
  }

  _forceReconnect() {
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
    }
    this._cleanup();
    this._connectWs();
  }

  _cleanup() {
    this._wsConnected = false;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._staleTimer) { clearInterval(this._staleTimer); this._staleTimer = null; }
    if (this._priceLogTimer) { clearInterval(this._priceLogTimer); this._priceLogTimer = null; }
    this._ws = null;
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    console.log(`[RTDS] reconnecting in ${this._reconnectDelay}ms...`);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._reconnectDelay = Math.min(this._reconnectDelay * 2, this._maxReconnectDelay);
      this._connectWs();
    }, this._reconnectDelay);
  }

  destroy() {
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._cleanup();
    }
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
  }
}

module.exports = RtdsClient;
