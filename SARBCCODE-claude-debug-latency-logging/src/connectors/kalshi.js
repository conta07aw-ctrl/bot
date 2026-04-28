/**
 * Kalshi connector — REST for auth + market discovery, WebSocket for real-time prices.
 *
 * Flow:
 *   1. REST: connect() validates credentials, getMarkets() discovers 15-min tickers
 *   2. WS:   subscribeWs(tickers) opens persistent stream for live price updates
 *   3. On each WS tick → emits 'tick' event for the Monitor to react instantly
 */

const crypto = require('crypto');
const https = require('https');
const axios = require('axios');
const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');

// Persistent HTTPS agent — reuses TCP+TLS connections across requests.
// Without this, every _get/_post/_delete opens a new socket, paying the
// full TLS handshake (~200-600ms to Kalshi from NYC3). keepAlive lets
// subsequent requests reuse the warm socket, dropping latency to ~50-100ms.
const kalshiAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 15_000,   // TCP keepalive probe interval
  maxSockets: 4,            // enough for parallel order + cancel
  maxFreeSockets: 2,        // keep 2 idle sockets warm between trades
});

const API_BASE = '/trade-api/v2';

// Any orderbook level with size below this is treated as empty. Kalshi
// trades in whole or tenth contracts, so real sizes never go below ~0.01;
// 1e-6 sits comfortably above float noise (1e-14) and below any real fill.
const SIZE_EPSILON = 1e-6;

class KalshiConnector extends EventEmitter {
  constructor() {
    super();
    this.baseUrl = config.kalshi.baseUrl;
    this.wsUrl = config.kalshi.wsUrl;
    this.apiKey = '';      // per-user, set via loadUserKeys()
    this.privateKey = '';  // per-user, set via loadUserKeys()
    this.connected = false;
    this.markets = new Map(); // ticker → normalized market

    // WebSocket state
    this._ws = null;
    this._wsConnected = false;
    this._subscribedTickers = new Set();
    this._reconnectDelay = 1000;
    this._maxReconnectDelay = 30000;
    this._reconnectTimer = null;
    this._pingTimer = null;
    this._lastPongAt = 0;  // tracks WS health — updated on every pong response
    this._cmdId = 0;

    // Per-ticker mini orderbook. Populated in real-time by the WS
    // orderbook_snapshot and orderbook_delta channels, seeded once via REST
    // on subscribeWs() so the first scan has fresh data before the WS
    // snapshot lands.
    //
    // ticker → { yes: Map<priceCents, size>, no: Map<priceCents, size> }
    //
    // Kalshi's orderbook (yes_dollars/no_dollars) contains mixed bids+asks
    // on each side. Bid/ask prices must come from the ticker channel
    // (yes_ask_dollars, no_ask_dollars, etc.), NOT from book derivations.
    // The book is maintained for depth/liquidity estimation and WS
    // freshness tracking only.
    this._books = new Map();

    // Per-ticker last-tick wall-clock timestamp (ms). Used by the dispatcher's
    // WS-livecheck fast path to refuse stale cache reads.
    // ticker → ms
    this._lastTickAt = new Map();

    // DIAGNOSTIC: throttle for "invalid book" warnings (sum of yAsk+nAsk < 0.98
    // is mathematically impossible in a binary market). Kept for one more
    // deploy cycle to verify the phantom-level fix holds in production.
    // ticker → last log ts.
    this._lastInvalidBookLog = new Map();
  }

  /* ---------------------------------------------------------- */
  /*  Authentication — RSA-PSS SHA-256 request signing           */
  /* ---------------------------------------------------------- */

  _sign(timestampMs, method, path) {
    const pathWithoutQuery = path.split('?')[0];
    const message = `${timestampMs}${method}${pathWithoutQuery}`;
    const signature = crypto.sign('sha256', Buffer.from(message), {
      key: this.privateKey,
      padding: crypto.constants.RSA_PKCS1_PSS_PADDING,
      saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST,
    });
    return signature.toString('base64');
  }

  _authHeaders(method, path) {
    const ts = Date.now().toString();
    return {
      'KALSHI-ACCESS-KEY': this.apiKey,
      'KALSHI-ACCESS-SIGNATURE': this._sign(ts, method, path),
      'KALSHI-ACCESS-TIMESTAMP': ts,
      'Content-Type': 'application/json',
    };
  }

  async _get(path) {
    const url = `${this.baseUrl}${path}`;
    const resp = await axios.get(url, {
      headers: this._authHeaders('GET', path),
      timeout: 10_000,
      httpsAgent: kalshiAgent,
    });
    return resp.data;
  }

  async _post(path, body) {
    const url = `${this.baseUrl}${path}`;
    const resp = await axios.post(url, body, {
      headers: this._authHeaders('POST', path),
      timeout: 10_000,
      httpsAgent: kalshiAgent,
    });
    return resp.data;
  }

  async _delete(path) {
    const url = `${this.baseUrl}${path}`;
    const resp = await axios.delete(url, {
      headers: this._authHeaders('DELETE', path),
      timeout: 10_000,
      httpsAgent: kalshiAgent,
    });
    return resp.data;
  }

  /**
   * Authenticated GET using a specific user's keys (for per-user balance checks).
   */
  async _getWithKeys(path, apiKey, privateKeyPem) {
    const ts = Date.now().toString();
    const pathWithoutQuery = path.split('?')[0];
    const message = `${ts}GET${pathWithoutQuery}`;
    const signature = crypto.sign('sha256', Buffer.from(message), {
      key: privateKeyPem,
      padding: crypto.constants.RSA_PKCS1_PSS_PADDING,
      saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST,
    });

    const url = `${this.baseUrl}${path}`;
    const resp = await axios.get(url, {
      headers: {
        'KALSHI-ACCESS-KEY': apiKey,
        'KALSHI-ACCESS-SIGNATURE': signature.toString('base64'),
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'Content-Type': 'application/json',
      },
      timeout: 10_000,
      httpsAgent: kalshiAgent,
    });
    return resp.data;
  }

  /**
   * Load user's API keys into this connector and reconnect.
   */
  async loadUserKeys(keys) {
    if (!keys?.kalshiApiKey || !keys?.kalshiPrivateKeyPem) return;
    if (this.connected && this.apiKey) return; // already loaded

    this.apiKey = keys.kalshiApiKey;
    this.privateKey = keys.kalshiPrivateKeyPem;
    console.log('[Kalshi] loading user keys and connecting...');
    await this.connect();
  }

  /* ---------------------------------------------------------- */
  /*  REST connection                                            */
  /* ---------------------------------------------------------- */

  async connect() {
    if (!this.apiKey || !this.privateKey) {
      console.log('[Kalshi] no API credentials — running in dry-run mode');
      this.connected = false;
      return;
    }

    try {
      const data = await this._get(`${API_BASE}/exchange/status`);
      console.log('[Kalshi] connected — exchange:', data.exchange_active ? 'active' : 'inactive');
      this.connected = true;
    } catch (err) {
      console.error('[Kalshi] connection failed:', err.response?.data?.message || err.message);
      this.connected = false;
    }
  }

  /* ---------------------------------------------------------- */
  /*  WebSocket — real-time price streaming                      */
  /* ---------------------------------------------------------- */

  /**
   * Open WebSocket and subscribe to ticker updates for given market tickers.
   * Emits 'tick' event on each price update: { ticker, yesAsk, yesBid, noAsk, noBid, ... }
   */
  async subscribeWs(marketTickers = []) {
    if (!this.apiKey || !this.privateKey) {
      console.log('[Kalshi WS] no credentials — skipping WebSocket');
      return;
    }

    // Track tickers to subscribe
    for (const t of marketTickers) this._subscribedTickers.add(t);

    // Seed each new ticker's mini-book ONCE via REST, AWAITING the fetch so
    // the caller knows the cache is orderbook-fresh before it runs any scan.
    // After this warm-start, the WS orderbook_snapshot + orderbook_delta
    // channels take over as the authoritative source of truth.
    //
    // CRITICAL: the Kalshi `/markets` endpoint (used by getMarkets) returns
    // yes/no ask fields that are NOT equivalent to the top-of-book from
    // `/markets/{ticker}/orderbook`. In practice they can differ by 5-10¢
    // because the markets endpoint serves a cached/derived quote. If the
    // monitor calls _runScan right after getMarkets without awaiting a real
    // orderbook fetch, it scans phantom prices and generates signals that
    // die 1 second later in the dispatcher pre-check.
    await Promise.all(
      marketTickers.map((t) => this._refreshOrderbookForTicker(t).catch(() => {}))
    );

    if (this._ws && this._wsConnected) {
      // Already connected — just send new subscription
      this._sendSubscribe(marketTickers);
      return;
    }

    if (this._ws) return; // connecting in progress

    this._connectWs();
  }

  /**
   * Fetch /orderbook for one ticker and rebuild that ticker's mini-book + cached fields.
   * Called once on subscribe as a warm start before the WS snapshot lands.
   */
  async _refreshOrderbookForTicker(ticker) {
    const book = await this.getOrderbook(ticker, { verbose: false });
    if (!book) return;

    const miniBook = { yes: new Map(), no: new Map() };
    for (const lvl of book.yes) {
      const cents = Math.round(lvl.price * 100);
      if (cents > 0 && cents < 100 && lvl.qty > 0) miniBook.yes.set(cents, lvl.qty);
    }
    for (const lvl of book.no) {
      const cents = Math.round(lvl.price * 100);
      if (cents > 0 && cents < 100 && lvl.qty > 0) miniBook.no.set(cents, lvl.qty);
    }

    this._books.set(ticker, miniBook);
    this._lastTickAt.set(ticker, Date.now());
    this._applyBookToCached(ticker);
  }

  _connectWs() {
    try {
      // Kalshi WS requires auth headers in the HTTP upgrade handshake
      const ts = Date.now().toString();
      const wsPath = '/trade-api/ws/v2';
      const sig = this._sign(ts, 'GET', wsPath);

      this._ws = new WebSocket(this.wsUrl, {
        headers: {
          'KALSHI-ACCESS-KEY': this.apiKey,
          'KALSHI-ACCESS-SIGNATURE': sig,
          'KALSHI-ACCESS-TIMESTAMP': ts,
        },
      });
    } catch (err) {
      console.error('[Kalshi WS] failed to create connection:', err.message);
      this._scheduleReconnect();
      return;
    }

    this._ws.on('open', () => {
      console.log('[Kalshi WS] connected + authenticated');
      this._wsConnected = true;
      this._lastPongAt = Date.now();
      this._reconnectDelay = 1000;

      // Already authenticated via headers — subscribe directly
      if (this._subscribedTickers.size > 0) {
        this._sendSubscribe([...this._subscribedTickers]);
      }
      this._startPing();
    });

    this._ws.on('pong', () => {
      this._lastPongAt = Date.now();
    });

    this._ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        this._handleWsMessage(msg);
      } catch (e) { console.error('[Kalshi] WS handler error:', e.message); }
    });

    this._ws.on('close', (code) => {
      console.log(`[Kalshi WS] closed (code: ${code})`);
      this._wsCleanup();
      this._scheduleReconnect();
    });

    this._ws.on('error', (err) => {
      console.error('[Kalshi WS] error:', err.message);
    });
  }

  _handleWsMessage(msg) {
    // Subscription confirmation
    if (msg.type === 'subscribed') {
      console.log('[Kalshi WS] subscription confirmed:', msg.msg?.channel || '');
      return;
    }

    // Error responses
    if (msg.type === 'error') {
      console.error('[Kalshi WS] error:', msg.msg || msg);
      return;
    }

    // Ticker channel updates
    if (msg.type === 'ticker' && msg.msg) {
      const tick = msg.msg;
      const ticker = tick.market_ticker;
      if (!ticker) return;

      // Update cached market — handle both _dollars (strings) and legacy (cents) formats
      const cached = this.markets.get(ticker);
      if (cached) {
        // PRICE FIELDS: ALWAYS update from the ticker channel. The ticker
        // provides Kalshi's authoritative best bid/ask (BBO) quotes.
        //
        // Previously, these were suppressed when a WS orderbook existed, and
        // asks were derived from the book via binary complement (1 - oppBid).
        // Diagnostic data (April 2026) proved the orderbook's yes_dollars /
        // no_dollars contain mixed bids+asks — the highest level on a side
        // can be an ask, not a bid. Deriving from that produced prices ~1¢
        // below the real ask, causing limit orders to rest with 0 fills.
        //
        // The ticker's yes_ask_dollars / no_ask_dollars are the correct
        // market ask prices and must always be trusted over book derivations.
        if (tick.yes_ask_dollars != null) cached.yesAsk = parseFloat(tick.yes_ask_dollars);
        else if (tick.yes_ask != null) cached.yesAsk = tick.yes_ask / 100;

        if (tick.yes_bid_dollars != null) cached.yesBid = parseFloat(tick.yes_bid_dollars);
        else if (tick.yes_bid != null) cached.yesBid = tick.yes_bid / 100;

        if (tick.no_ask_dollars != null) cached.noAsk = parseFloat(tick.no_ask_dollars);
        else if (tick.no_ask != null) cached.noAsk = tick.no_ask / 100;

        if (tick.no_bid_dollars != null) cached.noBid = parseFloat(tick.no_bid_dollars);
        else if (tick.no_bid != null) cached.noBid = tick.no_bid / 100;

        // Derive No from Yes if No wasn't provided (binary market complement)
        if (tick.no_ask_dollars == null && tick.no_ask == null && cached.yesBid > 0) {
          cached.noAsk = Math.round((1 - cached.yesBid) * 10000) / 10000;
        }
        if (tick.no_bid_dollars == null && tick.no_bid == null && cached.yesAsk > 0) {
          cached.noBid = Math.round((1 - cached.yesAsk) * 10000) / 10000;
        }

        // METADATA FIELDS: always update from ticker (the book doesn't carry these)
        if (tick.last_price_dollars != null) cached.lastPrice = parseFloat(tick.last_price_dollars);
        else if (tick.last_price != null) cached.lastPrice = tick.last_price / 100;

        if (tick.volume_dollars != null) cached.volume = parseFloat(tick.volume_dollars);
        else if (tick.volume != null) cached.volume = tick.volume;

        cached.lastUpdated = new Date().toISOString();

        // Update the freshness timestamp so the dispatcher's WS stale gate
        // recognises ticker-driven price updates — not just book events.
        // Without this, a ticker arriving 10ms ago would still show as
        // "stale" if the last book event was 500ms+ ago.
        this._lastTickAt.set(ticker, Date.now());

        this.emit('tick', cached);
      }
      return;
    }

    // Orderbook snapshot — full book state, replaces local book entirely.
    // Kalshi sends this immediately after subscribing to orderbook_delta.
    //
    // REAL payload shape (captured from live WS, April 2026):
    //   { market_ticker, market_id,
    //     yes_dollars_fp: [["0.6400","13.00"], ["0.6500","113.00"], ...],
    //     no_dollars_fp:  [["0.2300","13998.00"], ...] }
    //
    // Price comes as a dollar-string ("0.6400" → 64 cents) and size is a
    // decimal-string (Kalshi allows fractional sizes). Levels below 1¢ are
    // rounded away — the protocol sometimes shows stub levels at 0.001.
    if (msg.type === 'orderbook_snapshot' && msg.msg) {
      const snap = msg.msg;
      const ticker = snap.market_ticker;
      if (!ticker) return;

      const book = { yes: new Map(), no: new Map() };
      const fill = (arr, target) => {
        if (!Array.isArray(arr)) return;
        for (const lvl of arr) {
          if (!Array.isArray(lvl) || lvl.length < 2) continue;
          const priceDollars = parseFloat(lvl[0]);
          const size = parseFloat(lvl[1]);
          if (!(priceDollars > 0) || !(size > 0)) continue;
          const cents = Math.round(priceDollars * 100);
          if (cents > 0 && cents < 100) target.set(cents, size);
        }
      };
      fill(snap.yes_dollars_fp, book.yes);
      fill(snap.no_dollars_fp, book.no);

      // Defensive: if the snapshot somehow parsed to an empty book (unknown
      // future schema change, malformed payload), DO NOT overwrite an
      // existing populated book — a stale book is still better than an
      // empty one, and the next delta/REST refresh will heal it.
      const newSize = book.yes.size + book.no.size;
      if (newSize === 0) {
        const existing = this._books.get(ticker);
        if (existing && (existing.yes.size + existing.no.size) > 0) {
          console.warn(`[Kalshi WS] ${ticker} orderbook_snapshot parsed to empty book — keeping existing book`);
          return;
        }
      }

      this._books.set(ticker, book);
      this._lastTickAt.set(ticker, Date.now());
      this._applyBookToCached(ticker);
      return;
    }

    // Orderbook delta — add/remove quantity at a single price level.
    //
    // REAL payload shape (captured from live WS, April 2026):
    //   { market_ticker, market_id,
    //     price_dollars: "0.6400",
    //     delta_fp: "-1.00",   // can be fractional, positive or negative
    //     side: "yes" | "no",
    //     ts: "..." }
    //
    // CRITICAL: we use an epsilon threshold (not `<= 0`) to decide when a
    // level is empty. Kalshi delivers fractional sizes as strings, and
    // `current + change` in JS floating-point can leave residue like
    // 7.1e-15 or 1.42e-14 instead of exact zero. A plain `<= 0` check
    // misses these, leaving phantom levels in the map that poison top-of-
    // book (the phantom looks like the best bid if it sits above real
    // activity). 1e-6 is well below any real size (sizes trade in whole or
    // tenth contracts) and well above float noise.
    if (msg.type === 'orderbook_delta' && msg.msg) {
      const delta = msg.msg;
      const ticker = delta.market_ticker;
      if (!ticker) return;

      let book = this._books.get(ticker);
      if (!book) {
        // No snapshot received yet — seed an empty book so subsequent deltas apply
        book = { yes: new Map(), no: new Map() };
        this._books.set(ticker, book);
      }

      const side = String(delta.side || '').toLowerCase();
      const priceDollars = parseFloat(delta.price_dollars);
      const change = parseFloat(delta.delta_fp);
      if (!(priceDollars > 0) || !Number.isFinite(change)) return;
      const price = Math.round(priceDollars * 100);
      if (!(price > 0) || price >= 100) return;

      const target = side === 'yes' ? book.yes : side === 'no' ? book.no : null;
      if (!target) return;

      const current = target.get(price) || 0;
      const next = current + change;
      if (next < SIZE_EPSILON) target.delete(price);
      else target.set(price, next);

      this._lastTickAt.set(ticker, Date.now());
      this._applyBookToCached(ticker);
      return;
    }
  }

  /**
   * Read top-of-book for a ticker directly from the in-memory WS mini-book.
   * Returns null if no book exists or both sides are empty.
   *
   * Output is normalized to MATCH the dispatcher's parsed REST shape:
   *   { yes: [{price, qty}], no: [{price, qty}], ageMs }
   *
   * Both sides are sorted descending (highest bid first) and capped to top 5
   * — that's all the dispatcher uses (it derives ask from opposite side's
   * best bid via the binary complement). Prices are returned as DOLLARS
   * (0.01–0.99) since the internal Map stores cents (1–99).
   */
  getLiveSnapshot(ticker) {
    if (!ticker) return null;
    const book = this._books.get(ticker);
    if (!book) return null;
    if (book.yes.size === 0 && book.no.size === 0) return null;

    const toLevels = (sideMap) =>
      [...sideMap.entries()]
        .filter(([cents, size]) => cents > 0 && cents < 100 && size >= SIZE_EPSILON)
        .map(([cents, size]) => ({ price: cents / 100, qty: size }))
        .sort((a, b) => b.price - a.price)
        .slice(0, 10);

    const lastAt = this._lastTickAt.get(ticker) || 0;
    const ageMs = lastAt > 0 ? Date.now() - lastAt : -1;

    // wsAlive: true if the WS received a pong within the last 20s.
    // Proves the connection is alive even when the book is quiet.
    const wsAlive = this._wsConnected && (Date.now() - this._lastPongAt) < 20_000;

    return {
      yes: toLevels(book.yes),
      no: toLevels(book.no),
      ageMs,
      wsAlive,
    };
  }

  /**
   * Recompute top-of-book from the stored mini book and update the cached market.
   *
   * The book's yes_dollars / no_dollars contain mixed bids+asks — the
   * highest level on a side can be an ask, not a bid. The old pure
   * complement derivation (1 - oppHighest) picked up these phantom asks
   * and produced prices ~1¢ below the real ask, causing 100% resting.
   *
   * Hybrid approach: derive asks from the book in real-time, but floor
   * them at the ticker channel's last known ask (the authoritative BBO).
   *   bookDerived = 1 - highestOppLevel   (real-time, but can be too low)
   *   tickerAsk   = cached.yesAsk/noAsk   (correct, but fires on trades)
   *   final       = max(bookDerived, tickerAsk)
   *
   * This gives real-time updates when the market moves UP, and protects
   * against mixed-data errors when the book has ask-levels on the bid side.
   * When the market moves DOWN, the ticker catches up on the next trade.
   */
  _applyBookToCached(ticker) {
    const book = this._books.get(ticker);
    const cached = this.markets.get(ticker);
    if (!book || !cached) return;

    // Purge phantom levels (floating-point residue) before picking top-of-book.
    for (const [p, size] of book.yes) if (size < SIZE_EPSILON) book.yes.delete(p);
    for (const [p, size] of book.no) if (size < SIZE_EPSILON) book.no.delete(p);

    // Highest level on each side (could be a bid OR an ask — we don't know)
    let highestYesCents = 0;
    for (const p of book.yes.keys()) if (p > highestYesCents) highestYesCents = p;
    let highestNoCents = 0;
    for (const p of book.no.keys()) if (p > highestNoCents) highestNoCents = p;

    // Derive asks via binary complement, then floor at ticker's last known ask.
    // max() ensures we never send a limit price below the real ask (resting),
    // while still getting real-time updates when the market moves up.
    if (highestNoCents > 0) {
      const bookYesAsk = Math.round(100 - highestNoCents) / 100;
      cached.yesAsk = Math.max(bookYesAsk, cached.yesAsk || 0);
    }
    if (highestYesCents > 0) {
      const bookNoAsk = Math.round(100 - highestYesCents) / 100;
      cached.noAsk = Math.max(bookNoAsk, cached.noAsk || 0);
    }

    // Update bids: use highest level, floored by ticker bid (min = conservative).
    // Bids are used for signal generation, not order placement, so less critical.
    if (highestYesCents > 0) {
      cached.yesBid = highestYesCents / 100;
    }
    if (highestNoCents > 0) {
      cached.noBid = highestNoCents / 100;
    }

    // DIAGNOSTIC: check ask consistency.
    const askSum = (cached.yesAsk || 0) + (cached.noAsk || 0);
    if (askSum > 0 && askSum < 0.98) {
      const now = Date.now();
      const lastLog = this._lastInvalidBookLog.get(ticker) || 0;
      if (now - lastLog > 2000) {
        this._lastInvalidBookLog.set(ticker, now);
        const top5 = (m) => [...m.entries()].sort((a, b) => b[0] - a[0]).slice(0, 5);
        console.warn(
          `[Kalshi INVALID BOOK] ${ticker} askSum=${askSum.toFixed(4)} ` +
          `yesAsk=${cached.yesAsk} noAsk=${cached.noAsk} ` +
          `yesBid=${cached.yesBid} noBid=${cached.noBid} | ` +
          `yesTop5=${JSON.stringify(top5(book.yes))} ` +
          `noTop5=${JSON.stringify(top5(book.no))} ` +
          `yesSize=${book.yes.size} noSize=${book.no.size}`
        );
      }
    }

    cached.lastUpdated = new Date().toISOString();
    this.emit('tick', cached);
  }

  _sendSubscribe(tickers) {
    if (!tickers.length) return;

    console.log(`[Kalshi WS] subscribing to ${tickers.length} tickers: ${tickers.join(', ')}`);

    // Subscribe to ticker channel for price updates
    this._wsSend({
      id: ++this._cmdId,
      cmd: 'subscribe',
      params: {
        channels: ['ticker', 'orderbook_delta'],
        market_tickers: tickers,
      },
    });
  }

  /**
   * Unsubscribe from tickers no longer needed (e.g. expired 15-min window).
   */
  unsubscribeTickers(tickers) {
    for (const t of tickers) {
      this._subscribedTickers.delete(t);
      // Clear the mini book so stale bid levels from a finished round
      // don't bleed into the next round's top-of-book derivations.
      this._books.delete(t);
    }

    // Kalshi WS unsubscribe requires subscription IDs (sids), not market_tickers.
    // Since we don't track sids, just close the connection — it will reconnect
    // with only the new tickers when subscribeWs() is called next.
    if (this._wsConnected && this._subscribedTickers.size === 0) {
      this.destroy();
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
    }, 15_000);
  }

  _wsCleanup() {
    this._wsConnected = false;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      this._ws.removeAllListeners();
      this._ws = null;
    }
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;

    console.log(`[Kalshi WS] reconnecting in ${this._reconnectDelay}ms...`);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._reconnectDelay = Math.min(this._reconnectDelay * 2, this._maxReconnectDelay);
      this._connectWs();
    }, this._reconnectDelay);
  }

  /* ---------------------------------------------------------- */
  /*  Market fetching — REST (for discovery only)                */
  /* ---------------------------------------------------------- */

  async getMarkets(asset) {
    if (!this.connected) return [];

    const seriesTicker = asset.kalshi?.seriesTicker;
    if (!seriesTicker) return [];

    const allRaw = [];
    let cursor = null;

    try {
      do {
        let path = `${API_BASE}/markets?status=open&series_ticker=${seriesTicker}&limit=200`;
        if (cursor) path += `&cursor=${cursor}`;

        const data = await this._get(path);
        const markets = data.markets || [];
        allRaw.push(...markets);
        cursor = data.cursor || null;
      } while (cursor);
    } catch (err) {
      console.error(`[Kalshi] getMarkets(${asset.symbol}) error:`, err.response?.data?.message || err.message);
      return [];
    }

    // Group markets by event_ticker to find distinct 15-min events
    const eventGroups = new Map();
    for (const m of allRaw) {
      const et = m.event_ticker || '';
      if (!eventGroups.has(et)) eventGroups.set(et, []);
      eventGroups.get(et).push(m);
    }

    // Find the nearest future event (closest expiration)
    // The API already filters status=open, so all returned markets are active.
    // We only skip events where ALL markets have prices pinned at 0.99+ or 0.01-
    // (truly resolved), NOT events with just a few extreme prices.
    const now = new Date();
    let nearestEvent = null;
    let nearestExpiry = null;

    for (const [eventTicker, markets] of eventGroups) {
      const expStr = markets[0]?.expiration_time || markets[0]?.close_time || '';
      if (!expStr) continue;

      const expiry = new Date(expStr);
      if (expiry <= now) continue;

      // Only skip if ALL markets with quotes are pinned at extreme prices (truly settled)
      const withQuotes = markets.filter((m) => {
        const yes = parseFloat(m.yes_ask_dollars || m.yes_ask) || 0;
        return yes > 0;
      });
      const pinnedCount = withQuotes.filter((m) => {
        const yes = parseFloat(m.yes_ask_dollars || m.yes_ask) || 0;
        return yes >= 0.99 || yes <= 0.01;
      }).length;

      // Only skip if there are quotes AND every single one is pinned
      if (withQuotes.length > 0 && pinnedCount === withQuotes.length) {
        console.log(`[Kalshi] skipping fully settled event ${eventTicker} (${pinnedCount}/${withQuotes.length} pinned)`);
        continue;
      }

      if (!nearestExpiry || expiry < nearestExpiry) {
        nearestExpiry = expiry;
        nearestEvent = { eventTicker, markets };
      }
    }

    if (!nearestEvent) {
      console.log(`[Kalshi] ${asset.symbol} — no active events found (${eventGroups.size} events checked, all settled or expired)`);
      return [];
    }

    const normalized = nearestEvent.markets.map((m) => this._normalize(m, asset));

    // Cache
    for (const m of normalized) {
      this.markets.set(m.ticker, m);
    }

    console.log(`[Kalshi] ${asset.symbol} — event: ${nearestEvent.eventTicker}, expires: ${nearestExpiry.toISOString()}, strikes: ${normalized.length}`);

    return normalized;
  }

  /**
   * Fetch the live orderbook for a ticker.
   *
   * @param {string} ticker
   * @param {{verbose?: boolean}} [opts] - set verbose:false to suppress the
   *   per-call log line. The dispatcher wants the log; the 1s REST poll does
   *   not (would be log spam).
   */
  async getOrderbook(ticker, { verbose = true } = {}) {
    if (!this.connected) return null;
    try {
      const data = await this._get(`${API_BASE}/markets/${ticker}/orderbook`);

      // Kalshi v2 returns { orderbook_fp: { yes_dollars: [["price","qty"], ...], no_dollars: [...] } }
      const fp = data.orderbook_fp || data.orderbook || data;

      // Normalize to { yes: [{price, qty}], no: [{price, qty}] }
      const normalize = (arr) => (arr || []).map(entry => ({
        price: parseFloat(entry[0]),
        qty: parseFloat(entry[1]),
      })).filter(e => e.price > 0 && e.qty > 0);

      const book = {
        yes: normalize(fp.yes_dollars || fp.yes),
        no: normalize(fp.no_dollars || fp.no),
      };

      if (verbose) {
        // Log the REAL best bid (highest price), not the first raw-order entry.
        // The REST response is unordered, so book.yes[0] was often a 0.001 stub
        // and confused debugging — it looked like the book was empty.
        const bestYes = book.yes.reduce((m, l) => l.price > m ? l.price : m, 0);
        const bestNo = book.no.reduce((m, l) => l.price > m ? l.price : m, 0);
        console.log(`[Kalshi] orderbook(${ticker}): yes=${book.yes.length} levels, no=${book.no.length} levels | best yes bid=${bestYes || '-'} best no bid=${bestNo || '-'}`);
      }
      return book;
    } catch (err) {
      if (verbose) {
        console.error(`[Kalshi] getOrderbook(${ticker}) error:`, err.response?.data?.message || err.message);
      }
      return null;
    }
  }

  /* ---------------------------------------------------------- */
  /*  Normalization                                              */
  /* ---------------------------------------------------------- */

  _normalize(raw, asset) {
    // Kalshi API returns prices as dollar strings (e.g. "0.6500") with _dollars suffix
    const yesAsk = parseFloat(raw.yes_ask_dollars || raw.yes_ask) || 0;
    const yesBid = parseFloat(raw.yes_bid_dollars || raw.yes_bid) || 0;
    let noAsk = parseFloat(raw.no_ask_dollars || raw.no_ask) || 0;
    let noBid = parseFloat(raw.no_bid_dollars || raw.no_bid) || 0;
    const lastPrice = parseFloat(raw.last_price_dollars || raw.last_price) || 0;

    // Derive No from Yes complement if not provided (binary market)
    if (noAsk === 0 && yesBid > 0) noAsk = Math.round((1 - yesBid) * 10000) / 10000;
    if (noBid === 0 && yesAsk > 0) noBid = Math.round((1 - yesAsk) * 10000) / 10000;
    const volume = parseFloat(raw.volume_dollars || raw.volume) || 0;

    return {
      platform: 'kalshi',
      asset: asset.symbol,
      ticker: raw.ticker,
      title: raw.title || raw.subtitle || raw.ticker,
      yesAsk,
      yesBid,
      noAsk,
      noBid,
      lastPrice,
      volume,
      openInterest: raw.open_interest ?? 0,
      eventTicker: raw.event_ticker || '',
      strike: raw.floor_strike ?? null,
      expirationDate: raw.expiration_time || raw.close_time || '',
      lastUpdated: new Date().toISOString(),
    };
  }

  /* ---------------------------------------------------------- */
  /*  Order placement                                            */
  /* ---------------------------------------------------------- */

  async placeOrder(order) {
    if (!this.connected) {
      console.error('[Kalshi] cannot place order — not connected');
      return null;
    }

    const clientOrderId = `sarb-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    // Price cap in cents — protects arb margin (never pay more than expected).
    // Used as buy_max_cost for market orders and as limit price for sells.
    const priceCents = Math.round(order.price * 100); // Kalshi uses cents (1-99)
    if (priceCents < 1 || priceCents > 99) {
      console.error(`[Kalshi] invalid price: ${priceCents}¢ (from ${order.price})`);
      return null;
    }

    const action = order.action || 'buy';
    const body = {
      ticker: order.ticker,
      side: order.side,
      action,
      client_order_id: clientOrderId,
      count: order.count,
    };

    body.type = 'limit';
    body.time_in_force = 'immediate_or_cancel';
    if (order.side === 'yes') {
      body.yes_price = priceCents;
    } else {
      body.no_price = priceCents;
    }

    const t0 = Date.now();
    try {
      const path = `${API_BASE}/portfolio/orders`;
      const result = await this._post(path, body);
      const latencyMs = Date.now() - t0;
      const orderId = result.order?.order_id || clientOrderId;
      const status = result.order?.status;
      // Kalshi API v2 uses fill_count_fp (string) not count_filled
      const fillCount = Math.round(parseFloat(result.order?.fill_count_fp) || 0);
      const remainCount = Math.round(parseFloat(result.order?.remaining_count_fp) || 0);

      console.log(`[Kalshi] order placed (${latencyMs}ms): ${action} ${body.type}/${body.time_in_force || 'n/a'} ${order.side}@${priceCents}¢ x${order.count} on ${order.ticker} — id: ${orderId} status: ${status} filled: ${fillCount}/${order.count}`);

      if (status === 'executed') {
        result._actualFilled = fillCount > 0 ? fillCount : order.count;
        return result;
      }

      if (status === 'canceled' && fillCount > 0) {
        console.log(`[Kalshi] IOC partial fill: ${fillCount}/${order.count} filled, rest canceled`);
        result._actualFilled = fillCount;
        return result;
      }

      // Market orders should never rest — they fill or reject instantly.
      // Keep the resting handler as a safety net for sell limits and any
      // unexpected API behavior.
      if (status === 'resting' || (status !== 'executed' && remainCount > 0)) {
        console.warn(`[Kalshi] order RESTING (filled ${fillCount}/${order.count}) — cancelling immediately`);
        const cancelResult = await this.cancelOrder(orderId);

        // If cancel fails (not_found), the order may have been filled
        // between status check and cancel. Fetch fresh status.
        if (cancelResult === null) {
          const freshOrder = await this.getOrder(orderId);
          if (freshOrder) {
            const freshFill = Math.round(parseFloat(freshOrder.order?.fill_count_fp || freshOrder.fill_count_fp) || 0);
            if (freshFill > 0) {
              console.warn(`[Kalshi] PHANTOM FILL: ${freshFill} contracts filled after cancel-not-found`);
              result.order = { ...result.order, ...freshOrder.order, count_filled: freshFill };
              result._actualFilled = freshFill;
              result._phantomFill = true;
              return result;
            }
          }
        }

        if (fillCount > 0) {
          console.log(`[Kalshi] partial fill: ${fillCount} contracts kept, rest cancelled`);
          result._actualFilled = fillCount;
          return result;
        }

        console.warn(`[Kalshi] order cancelled with 0 fills — treating as failure`);
      }
      return null;
    } catch (err) {
      const latencyMs = Date.now() - t0;
      console.error(`[Kalshi] placeOrder error (${latencyMs}ms):`, err.response?.data || err.message);
      return null;
    }
  }

  /**
   * Fetch a single order by ID. Used to check fill status when cancel fails.
   */
  async getOrder(orderId) {
    try {
      const path = `${API_BASE}/portfolio/orders/${orderId}`;
      const result = await this._get(path);
      return result;
    } catch (err) {
      console.error(`[Kalshi] getOrder(${orderId}) error:`, err.response?.data || err.message);
      return null;
    }
  }

  /**
   * Cancel an open/resting order by ID.
   * Returns the cancel result, or null if the order was already gone (not_found).
   */
  async cancelOrder(orderId) {
    try {
      const path = `${API_BASE}/portfolio/orders/${orderId}`;
      const result = await this._delete(path);
      console.log(`[Kalshi] order ${orderId} cancelled`);
      return result;
    } catch (err) {
      const errData = err.response?.data;
      const errMsg = errData?.message || errData?.error || err.message;
      const status = err.response?.status;
      console.error(`[Kalshi] cancelOrder error (HTTP ${status}): ${errMsg}`);
      // not_found (404) means the order was already executed or expired —
      // caller must check fill status via getOrder() to detect phantom fills.
      return null;
    }
  }

  /* ---------------------------------------------------------- */
  /*  Cleanup                                                    */
  /* ---------------------------------------------------------- */

  destroy() {
    if (this._ws) {
      this._ws.close();
      this._wsCleanup();
    }
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
  }
}

module.exports = KalshiConnector;
