/**
 * Polymarket connector — REST for market discovery, WebSocket for real-time prices.
 *
 * Flow:
 *   1. REST: connect() validates API, getMarkets() discovers 15-min markets via Gamma slug
 *   2. WS:   subscribeWs(assetIds) opens persistent stream for live price updates
 *   3. On each WS tick → emits 'tick' event for the Monitor to react instantly
 *
 * WebSocket endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
 */

const axios = require('axios');
const { ethers } = require('ethers');
const WebSocket = require('ws');
const EventEmitter = require('events');
const https = require('https');
const {
  ClobClient,
  Chain,
  createL2Headers,
  OrderType,
  Side,
  SignatureTypeV2,
} = require('@polymarket/clob-client-v2');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const { HttpsProxyAgent } = require('https-proxy-agent');
const config = require('../config');

const GAMMA_BASE = 'https://gamma-api.polymarket.com';
const CLOB_WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const INTERVAL_SECS = 900; // 15 minutes

const clobAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 10_000,
  maxSockets: 4,
  maxFreeSockets: 2,
});


class PolymarketConnector extends EventEmitter {
  constructor() {
    super();
    this.clobUrl = config.polymarket.baseUrl;
    this.apiKey = '';      // per-user, set via loadUserKeys()
    this.apiSecret = '';   // per-user, set via loadUserKeys()
    this.passphrase = '';  // per-user, set via loadUserKeys()
    this.connected = false;
    this.markets = new Map(); // conditionId → normalized market

    // Wallet for order signing (per-user, set via loadUserKeys())
    this.wallet = null;
    this.viemWallet = null;
    this.funderAddress = '';
    this._clobClient = null; // Official ClobClient instance for order placement

    // Per-market metadata cache (tokenId → { feeRateBps: string })
    // Polymarket rejects orders with feeRateBps=0 when the market has a
    // non-zero taker fee. We fetch via GET /fee-rate?token_id=X on first
    // use and cache forever (fee rate is immutable per market).
    this._feeRateCache = new Map();

    // WebSocket state
    this._ws = null;
    this._wsConnected = false;
    this._subscribedAssets = new Map(); // conditionId → { yesTokenId, noTokenId }
    this._reconnectDelay = 1000;
    this._maxReconnectDelay = 30000;
    this._reconnectTimer = null;
    this._pingTimer = null;
    this._lastPongAt = 0;  // tracks WS health — updated on every pong response

    // Per-token mini orderbook maintained from WS book + price_change events.
    // tokenId → { asks: Map<priceStr, size>, bids: Map<priceStr, size> }
    //
    // Without this, the WS handler used to set yesPrice from msg.price on every
    // event — including last_trade_price events — which made the decisionEngine
    // see prices that had never been the actual top-of-book. The result was a
    // flood of phantom signals that the dispatcher had to abort at REST time.
    this._books = new Map();

    // Per-token last-tick wall-clock timestamp (ms). Used by the dispatcher's
    // WS-livecheck fast path to refuse stale cache reads.
    // tokenId → ms
    this._lastTickAt = new Map();

    // Last order-placement error metadata. Populated by _postOrder on any
    // failure path (HTTP error, CLOB `error` field, or network exception).
    // The hedge reads this right after a failed placeOrder() to decide
    // whether to walk the book (liquidity), halve size (balance), or retry.
    //   { type: 'liquidity' | 'balance' | 'other',
    //     message: string,
    //     polyBalance: number|null,  // parsed from "balance: N" (6-decimal PUSD)
    //     polyRequired: number|null, // parsed from "order amount: N"
    //     httpStatus: number|null }
    this._lastOrderError = null;
  }

  /* ---------------------------------------------------------- */
  /*  Helpers                                                    */
  /* ---------------------------------------------------------- */

  _clobHeaders() {
    const h = { 'Content-Type': 'application/json' };
    if (this.apiKey) {
      h['POLY_API_KEY'] = this.apiKey;
      h['POLY_PASSPHRASE'] = this.passphrase;
    }
    return h;
  }

  _getProxyAgent() {
    if (!process.env.POLY_PROXY_URL) return null;
    if (!this._proxyAgent) {
      this._proxyAgent = new HttpsProxyAgent(process.env.POLY_PROXY_URL);
    }
    return this._proxyAgent;
  }

  async _gammaGet(path, params = {}) {
    // Gamma should be accessed directly. Routing it through the CLOB trading
    // proxy can return 407 (proxy auth) and blocks market discovery entirely.
    // Keep proxy usage for CLOB/auth order routes only.
    const opts = { params, timeout: 10_000 };
    const resp = await axios.get(`${GAMMA_BASE}${path}`, opts);
    return resp.data;
  }

  async _clobGet(path, params = {}) {
    const opts = {
      headers: this._clobHeaders(),
      params,
      timeout: 10_000,
    };
    const resp = await axios.get(`${this.clobUrl}${path}`, opts);
    return resp.data;
  }

  async _clobAuthGet(path, params = {}) {
    const qs = new URLSearchParams(params).toString();
    const fullUrl = qs ? `${this.clobUrl}${path}?${qs}` : `${this.clobUrl}${path}`;
    // HMAC signs only the path (no query params) for GET requests
    this._ensureViemWallet();
    const headers = await createL2Headers(
      this.viemWallet || this.wallet,
      { key: this.apiKey, secret: this.apiSecret, passphrase: this.passphrase },
      { method: 'GET', requestPath: path },
    );
    const opts = {
      headers,
      timeout: 10_000,
    };
    const resp = await axios.get(fullUrl, opts);
    return resp.data;
  }

  async getTokenBalance(tokenId) {
    if (!this.apiKey || !this.wallet) return null;
    try {
      const sigType = this.funderAddress
        && this.funderAddress.toLowerCase() !== this.wallet.address.toLowerCase()
        ? SignatureTypeV2.GNOSIS_SAFE : SignatureTypeV2.EOA;
      const data = await this._clobAuthGet('/balance-allowance', {
        asset_type: 'CONDITIONAL',
        token_id: tokenId,
        signature_type: sigType,
      });
      if (data && data.balance != null) {
        const raw = parseFloat(data.balance);
        const scaled = raw / 1e6;
        if (this._balLogCount === undefined) this._balLogCount = 0;
        if (this._balLogCount < 5) {
          this._balLogCount++;
          console.log(`[Polymarket] getTokenBalance raw=${data.balance} parsed=${raw} scaled=${scaled}`);
        }
        if (scaled > 0) return scaled;
        if (raw > 0 && raw < 1000) return raw;
        return null;
      }
      return null;
    } catch (err) {
      if (this._balErrLogCount === undefined) this._balErrLogCount = 0;
      if (this._balErrLogCount < 5) {
        this._balErrLogCount++;
        console.warn(`[Polymarket] getTokenBalance error: ${err.response?.status || ''} ${err.message}`);
      }
      return null;
    }
  }

  _get15mTimestamps() {
    const nowSecs = Math.floor(Date.now() / 1000);
    const current = Math.floor(nowSecs / INTERVAL_SECS) * INTERVAL_SECS;
    const next = current + INTERVAL_SECS;
    return [current, next];
  }

  _ensureViemWallet() {
    if (!this.viemWallet && this.wallet) {
      const pk = this.wallet.privateKey.startsWith('0x') ? this.wallet.privateKey : `0x${this.wallet.privateKey}`;
      const account = privateKeyToAccount(pk);
      this.viemWallet = createWalletClient({ account, chain: polygon, transport: http() });
    }
    return this.viemWallet;
  }

  /**
   * Load user's API keys into this connector.
   */
  loadUserKeys(keys) {
    if (!keys?.polyApiKey) return;

    this.apiKey = keys.polyApiKey;
    this.apiSecret = keys.polyApiSecret || '';
    this.passphrase = keys.polyPassphrase || '';
    this.funderAddress = keys.polyFunderAddress || '';

    if (keys.polyPrivateKey) {
      try {
        this.wallet = new ethers.Wallet(keys.polyPrivateKey);
        if (!this.funderAddress) this.funderAddress = this.wallet.address;
        this._ensureViemWallet();
      } catch (err) {
        console.error('[Polymarket] invalid private key:', err.message);
      }
    }

    console.log(`[Polymarket] user keys loaded (apiKey: ${this.apiKey.slice(0, 8)}...)`);
  }

  /**
   * Fetch the taker fee rate (in basis points) for a given market.
   * Polymarket rejects orders where order.feeRateBps does not match the
   * market's configured takerFee, so we must fetch this value before
   * signing each order. Result is cached per-tokenId (fee is immutable).
   *
   * Endpoint: GET /fee-rate?token_id=...
   * Response: { base_fee: number }  (bps, e.g. 1000 = 10%)
   *
   * This is a READ, so it goes direct (no proxy) — no geoblock on reads.
   *
   * @param {string} tokenId - CLOB token ID
   * @returns {Promise<string|null>} fee rate as string (e.g. "1000"), or null on failure
   */
  async _getFeeRateBps(tokenId) {
    if (this._feeRateCache.has(tokenId)) {
      return this._feeRateCache.get(tokenId);
    }
    try {
      const resp = await axios.get(`${this.clobUrl}/fee-rate`, {
        params: { token_id: tokenId },
        headers: this._clobHeaders(),
        timeout: 10_000,
      });
      const baseFee = resp.data?.base_fee;
      if (baseFee === undefined || baseFee === null) {
        console.warn(`[Polymarket] _getFeeRateBps: no base_fee in response for ${tokenId.slice(0, 16)}... (${JSON.stringify(resp.data).slice(0, 120)})`);
        return null;
      }
      const feeStr = String(baseFee);
      this._feeRateCache.set(tokenId, feeStr);
      console.log(`[Polymarket] fee rate for ${tokenId.slice(0, 16)}...: ${feeStr} bps (cached)`);
      return feeStr;
    } catch (err) {
      const data = err.response?.data ? JSON.stringify(err.response.data) : err.message;
      console.error(`[Polymarket] _getFeeRateBps failed for ${tokenId.slice(0, 16)}...: ${data}`);
      return null;
    }
  }

  /**
   * Derive API keys from wallet using official Polymarket CLOB client.
   * This replaces Builder Codes — keys are derived from the private key.
   * Requires proxy to be set first (if on datacenter IP).
   */
  async deriveApiKeys() {
    if (!this.wallet) {
      console.error('[Polymarket] cannot derive API keys — no wallet');
      return false;
    }

    this._ensureViemWallet();
    if (!this.viemWallet) {
      console.error('[Polymarket] cannot derive API keys — viem wallet unavailable');
      return false;
    }

    const signerAddr = this.viemWallet.account.address.toLowerCase();
    const proxyAddr = (this.funderAddress || '').toLowerCase();
    const useProxy = proxyAddr && proxyAddr !== signerAddr;
    const signatureType = useProxy ? SignatureTypeV2.GNOSIS_SAFE : SignatureTypeV2.EOA;
    const clientConfig = {
      host: this.clobUrl,
      chain: Chain.POLYGON,
      signer: this.viemWallet,
      signatureType,
      funderAddress: useProxy ? this.funderAddress : undefined,
      retryOnError: true,
    };

    try {
      const client = new ClobClient(clientConfig);
      const creds = await client.createOrDeriveApiKey();
      console.log(`[Polymarket] createOrDeriveApiKey response: ${JSON.stringify(creds).slice(0, 200)}`);

      if (creds && creds.key) {
        this.apiKey = creds.key;
        this.apiSecret = creds.secret;
        this.passphrase = creds.passphrase;

        this._clobClient = new ClobClient({
          ...clientConfig,
          creds: {
            key: this.apiKey,
            secret: this.apiSecret,
            passphrase: this.passphrase,
          },
        });

        console.log(`[Polymarket] API keys derived successfully (apiKey: ${this.apiKey.slice(0, 8)}...)`);
        return true;
      }

      console.warn('[Polymarket] createOrDeriveApiKey returned empty — will use stored keys');
    } catch (err) {
      const data = err.response?.data ? JSON.stringify(err.response.data) : err.message;
      console.error(`[Polymarket] API key derivation failed: ${data}`);
    }

    if (!this._clobClient && this.apiKey && this.apiSecret) {
      try {
        this._clobClient = new ClobClient({
          ...clientConfig,
          creds: {
            key: this.apiKey,
            secret: this.apiSecret,
            passphrase: this.passphrase,
          },
        });
        console.log(`[Polymarket] ClobClient created with STORED keys (apiKey: ${this.apiKey.slice(0, 8)}...)`);
        return true;
      } catch (fallbackErr) {
        console.error(`[Polymarket] ClobClient fallback failed: ${fallbackErr.message}`);
      }
    }
    return false;
  }

  /* ---------------------------------------------------------- */
  /*  REST connection                                            */
  /* ---------------------------------------------------------- */

  async connect() {
    try {
      await this._gammaGet('/markets', { limit: 1 });
      console.log('[Polymarket] connected — Gamma API reachable');
      this.connected = true;
    } catch (err) {
      console.error('[Polymarket] connection failed:', err.response?.data?.message || err.message);
      this.connected = false;
    }
  }

  /* ---------------------------------------------------------- */
  /*  WebSocket — real-time price streaming via CLOB WS          */
  /* ---------------------------------------------------------- */

  /**
   * Subscribe to real-time price updates for given markets.
   * Each market needs its token IDs (yesTokenId, noTokenId) for subscription.
   */
  subscribeWs(markets = []) {
    // Track what to subscribe to
    for (const m of markets) {
      if (m.conditionId) {
        this._subscribedAssets.set(m.conditionId, {
          asset: m.asset,
          yesTokenId: m.yesTokenId,
          noTokenId: m.noTokenId,
          conditionId: m.conditionId,
        });
      }
    }

    if (this._ws && this._wsConnected) {
      this._sendSubscriptions(markets);
      return;
    }

    if (this._ws) return; // connecting in progress

    this._connectWs();
  }

  _connectWs() {
    try {
      // WS should stay direct. Proxy is reserved for order placement only.
      this._ws = new WebSocket(CLOB_WS_URL);
    } catch (err) {
      console.error('[Polymarket WS] failed to create connection:', err.message);
      this._scheduleReconnect();
      return;
    }

    this._ws.on('open', () => {
      console.log('[Polymarket WS] connected');
      this._wsConnected = true;
      this._lastPongAt = Date.now();
      this._reconnectDelay = 1000;

      // Subscribe to all tracked assets
      if (this._subscribedAssets.size > 0) {
        this._sendSubscriptions([...this._subscribedAssets.values()]);
      }

      this._startPing();
    });

    this._ws.on('pong', () => {
      this._lastPongAt = Date.now();
    });

    this._ws.on('message', (raw) => {
      try {
        const text = raw.toString().trim();
        if (!text) return;
        if (text[0] !== '{' && text[0] !== '[') {
          // CLOB occasionally sends plain-text protocol errors (e.g. "INVALID OPERATION").
          // These are not JSON payloads and should not crash/log-spam the WS handler.
          if (!this._lastNonJsonWsLogAt || (Date.now() - this._lastNonJsonWsLogAt) > 10_000) {
            this._lastNonJsonWsLogAt = Date.now();
            console.warn(`[Polymarket WS] non-JSON message ignored: ${text.slice(0, 120)}`);
          }
          return;
        }
        const msgs = JSON.parse(text);
        // Messages can be a single object or an array
        const arr = Array.isArray(msgs) ? msgs : [msgs];
        for (const msg of arr) {
          this._handleWsMessage(msg);
        }
      } catch (e) { console.error('[Poly] WS handler error:', e.message); }
    });

    this._ws.on('close', (code) => {
      console.log(`[Polymarket WS] closed (code: ${code})`);
      this._wsCleanup();
      this._scheduleReconnect();
    });

    this._ws.on('error', (err) => {
      console.error('[Polymarket WS] error:', err.message);
    });
  }

  _handleWsMessage(msg) {
    if (!msg) return;
    // Polymarket CLOB WS event types we care about:
    //   - "book"             → full snapshot { asset_id, bids:[{price,size}], asks:[{price,size}] }
    //                          asset_id is at the TOP LEVEL of the message.
    //   - "price_change"     → batch of deltas { market, price_changes:[{asset_id,price,side,size,...}] }
    //                          asset_id is on EACH item, NOT at top level. The top-level `market`
    //                          is the conditionId (hex), which is useless for token-level books.
    //                          NOTE: field is `price_changes` (plural), not `changes`.
    //   - "last_trade_price" → last executed trade price only — IGNORE (not a quote; was source
    //                          of phantom signals before the fix).
    //   - "tick_size_change" → ignored.
    const eventType = msg.event_type || msg.type || '';

    if (eventType === 'last_trade_price' || eventType === 'tick_size_change') return;

    // Track which token books were touched in this message so we can propagate once at the end.
    const touchedTokens = new Set();

    if (eventType === 'book' || Array.isArray(msg.asks) || Array.isArray(msg.bids)) {
      // Full snapshot for a single token. asset_id is at top level.
      const tokenId = msg.asset_id || msg.token_id || '';
      if (tokenId && this._applyBookSnapshot(tokenId, msg)) {
        touchedTokens.add(tokenId);
      }
    } else if (eventType === 'price_change' || Array.isArray(msg.price_changes) || Array.isArray(msg.changes)) {
      // Batch of deltas. Each item in price_changes carries its own asset_id.
      // Accept `price_changes` (current server format) with `changes` as legacy fallback.
      const items = Array.isArray(msg.price_changes) ? msg.price_changes
                  : Array.isArray(msg.changes) ? msg.changes
                  : [];
      for (const ch of items) {
        const tokenId = this._applyPriceChange(ch);
        if (tokenId) touchedTokens.add(tokenId);
      }
    } else {
      // Unknown / shape-only fallback: if it has a top-level asset_id and looks like a book,
      // try to handle it as a snapshot. Otherwise drop silently.
      const tokenId = msg.asset_id || '';
      if (tokenId && (Array.isArray(msg.asks) || Array.isArray(msg.bids))) {
        if (this._applyBookSnapshot(tokenId, msg)) touchedTokens.add(tokenId);
      }
      return;
    }

    // Propagate updated books to cached markets and emit ticks.
    const now = Date.now();
    for (const tokenId of touchedTokens) {
      this._lastTickAt.set(tokenId, now);
      this._propagateBook(tokenId);
    }

  }

  /**
   * Read top-of-book for a token directly from the in-memory WS mini-book.
   * Returns null if no book exists. Returns ageMs = wall-clock ms since the
   * last tick was applied — caller must enforce its own staleness threshold.
   *
   * Shape mirrors a parsed REST orderbook just enough for the dispatcher to
   * consume it identically:
   *   { bestAsk, bestAskSize, bestBid, bestBidSize, asks: [{price,size}], bids: [{price,size}], ageMs }
   *
   * Only the top 5 levels per side are returned — that's all the dispatcher
   * uses (size walking + DiagBook). The full Map stays in place for the
   * propagation logic.
   */
  getLiveSnapshot(tokenId) {
    if (!tokenId) return null;
    const book = this._books.get(tokenId);
    if (!book) return null;
    if (book.asks.size === 0 && book.bids.size === 0) return null;

    const sortedAsks = [...book.asks.entries()]
      .map(([p, s]) => ({ price: parseFloat(p), size: s }))
      .filter((l) => l.price > 0 && l.size > 0)
      .sort((a, b) => a.price - b.price)
      .slice(0, 5);

    const sortedBids = [...book.bids.entries()]
      .map(([p, s]) => ({ price: parseFloat(p), size: s }))
      .filter((l) => l.price > 0 && l.size > 0)
      .sort((a, b) => b.price - a.price)
      .slice(0, 5);

    const bestAsk = sortedAsks[0]?.price ?? null;
    const bestAskSize = sortedAsks[0]?.size ?? 0;
    const bestBid = sortedBids[0]?.price ?? null;
    const bestBidSize = sortedBids[0]?.size ?? 0;

    const lastAt = this._lastTickAt.get(tokenId) || 0;
    const ageMs = lastAt > 0 ? Date.now() - lastAt : -1;

    // wsAlive: true if the WS received a pong within the last 20s.
    // Proves the connection is alive even when the book is quiet.
    const wsAlive = this._wsConnected && (Date.now() - this._lastPongAt) < 20_000;

    return { bestAsk, bestAskSize, bestBid, bestBidSize, asks: sortedAsks, bids: sortedBids, ageMs, wsAlive };
  }

  /**
   * Apply a full book snapshot for one token. Replaces the local mini-book
   * with the server's view. Returns true if the book was touched.
   */
  _applyBookSnapshot(tokenId, msg) {
    if (!tokenId) return false;
    let book = this._books.get(tokenId);
    if (!book) {
      book = { asks: new Map(), bids: new Map() };
      this._books.set(tokenId, book);
    }
    book.asks.clear();
    book.bids.clear();
    const asksArr = Array.isArray(msg.asks) ? msg.asks : [];
    const bidsArr = Array.isArray(msg.bids) ? msg.bids : [];
    for (const lvl of asksArr) {
      const price = parseFloat(Array.isArray(lvl) ? lvl[0] : lvl.price);
      const size = parseFloat(Array.isArray(lvl) ? lvl[1] : lvl.size);
      if (price > 0 && size > 0) book.asks.set(price.toString(), size);
    }
    for (const lvl of bidsArr) {
      const price = parseFloat(Array.isArray(lvl) ? lvl[0] : lvl.price);
      const size = parseFloat(Array.isArray(lvl) ? lvl[1] : lvl.size);
      if (price > 0 && size > 0) book.bids.set(price.toString(), size);
    }
    return true;
  }

  /**
   * Apply one price_change delta item. The item carries its own asset_id
   * (which side/price/size to modify). Returns the tokenId touched, or ''.
   * size=0 removes the level.
   */
  _applyPriceChange(ch) {
    if (!ch) return '';
    const tokenId = ch.asset_id || ch.token_id || '';
    if (!tokenId) return '';
    const price = parseFloat(ch.price);
    const size = parseFloat(ch.size);
    if (!(price > 0) || isNaN(size)) return '';
    const sideStr = String(ch.side || '').toLowerCase();
    let book = this._books.get(tokenId);
    if (!book) {
      book = { asks: new Map(), bids: new Map() };
      this._books.set(tokenId, book);
    }
    const target = (sideStr === 'sell' || sideStr === 'ask') ? book.asks
                 : (sideStr === 'buy'  || sideStr === 'bid') ? book.bids
                 : null;
    if (!target) return '';
    if (size > 0) target.set(price.toString(), size);
    else target.delete(price.toString());
    return tokenId;
  }

  /**
   * Recompute best bid/ask for a token and push to any cached market that
   * references it (yes or no side). Emits 'tick' per market updated.
   */
  _propagateBook(tokenId) {
    const book = this._books.get(tokenId);
    if (!book) return;
    const bestAsk = this._bestPrice(book.asks, 'min');
    const bestBid = this._bestPrice(book.bids, 'max');

    for (const [conditionId, info] of this._subscribedAssets) {
      const cached = this.markets.get(conditionId);
      if (!cached) continue;

      let updated = false;
      if (tokenId === info.yesTokenId) {
        if (bestAsk != null) cached.yesAsk = bestAsk;
        if (bestBid != null) cached.yesBid = bestBid;
        if (bestAsk != null) cached.yesPrice = bestAsk;
        updated = true;
      } else if (tokenId === info.noTokenId) {
        if (bestAsk != null) cached.noAsk = bestAsk;
        if (bestBid != null) cached.noBid = bestBid;
        if (bestAsk != null) cached.noPrice = bestAsk;
        updated = true;
      }

      if (updated) {
        cached.lastUpdated = new Date().toISOString();
        this.emit('tick', cached);
      }
    }
  }

  _bestPrice(map, mode) {
    if (!map || map.size === 0) return null;
    let best = null;
    for (const k of map.keys()) {
      const p = parseFloat(k);
      if (!(p > 0)) continue;
      if (best == null) best = p;
      else if (mode === 'min' && p < best) best = p;
      else if (mode === 'max' && p > best) best = p;
    }
    return best;
  }

  _sendSubscriptions(markets) {
    const tokenIds = [];
    for (const m of markets) {
      if (m.yesTokenId) tokenIds.push(m.yesTokenId);
      if (m.noTokenId) tokenIds.push(m.noTokenId);
    }
    const deduped = [...new Set(tokenIds)];

    if (deduped.length === 0) return;

    console.log(`[Polymarket WS] subscribing to ${deduped.length} token streams`);

    // Polymarket CLOB WS subscription: keep payload minimal/strict.
    // Extra fields (e.g. custom_feature_enabled/auth stubs) can trigger
    // plain-text "INVALID OPERATION" frames on some gateway versions.
    this._wsSend({
      type: 'market',
      assets_ids: deduped,
    });
  }

  /**
   * Unsubscribe from markets no longer needed.
   */
  unsubscribeMarkets(conditionIds) {
    for (const id of conditionIds) {
      this._subscribedAssets.delete(id);
    }
    // Polymarket WS may not support explicit unsubscribe — new connection on rotation handles this
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
        const silentForMs = Date.now() - this._lastPongAt;
        if (this._lastPongAt > 0 && silentForMs > 45_000) {
          console.warn(`[Polymarket WS] pong timeout (${silentForMs}ms) — forcing reconnect`);
          // terminate() is safer than close() on half-open sockets.
          this._ws.terminate();
          return;
        }
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
    if (this._subscribedAssets.size === 0) return;

    console.log(`[Polymarket WS] reconnecting in ${this._reconnectDelay}ms...`);
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

    const slugPrefix = asset.polymarket?.slugPrefix;
    if (!slugPrefix) {
      console.log(`[Polymarket] ${asset.symbol} — no slugPrefix configured, skipping`);
      return [];
    }

    const [currentTs, nextTs] = this._get15mTimestamps();
    const slugs = [
      `${slugPrefix}-${currentTs}`,
      `${slugPrefix}-${nextTs}`,
    ];

    const allMarkets = [];

    for (const slug of slugs) {
      try {
        const events = await this._gammaGet('/events', { slug });
        const items = Array.isArray(events) ? events : [events];

        for (const event of items) {
          if (!event || !event.markets) continue;

          const eventMarkets = Array.isArray(event.markets) ? event.markets : [];
          for (const raw of eventMarkets) {
            const normalized = this._normalize(raw, asset);
            if (normalized.conditionId && !allMarkets.some((m) => m.conditionId === normalized.conditionId)) {
              allMarkets.push(normalized);
            }
          }
        }
      } catch (err) {
        if (err.response?.status !== 404) {
          console.debug(`[Polymarket] ${asset.symbol} slug=${slug} — ${err.response?.status || err.message}`);
        }
      }
    }

    if (allMarkets.length > 0) {
      console.log(`[Polymarket] ${asset.symbol} — found ${allMarkets.length} market(s): "${allMarkets[0].question}"`);
    } else {
      console.log(`[Polymarket] ${asset.symbol} — no 15-min market found (slugs: ${slugs.join(', ')})`);
    }

    // Strike is NOT fetched here anymore — RTDS client captures it in real-time.
    // The Monitor injects the strike from RTDS into each market after discovery.

    // Cache for WS tick matching (conditionId → market object).
    // REST is only used for discovery; WS handles all price updates after this.
    for (const m of allMarkets) {
      this.markets.set(m.conditionId, m);
    }

    return allMarkets;
  }

  async getOrderbook(tokenId) {
    if (!this.connected) return null;
    try {
      const data = await this._clobGet('/book', { token_id: tokenId });
      return data || null;
    } catch (err) {
      console.error(`[Polymarket] getOrderbook(${tokenId}) error:`, err.response?.data?.message || err.message);
      return null;
    }
  }

  async getPrices(tokenIds) {
    if (!this.connected || tokenIds.length === 0) return {};
    try {
      const data = await this._clobGet('/prices', { token_ids: tokenIds.join(',') });
      return data || {};
    } catch (err) {
      console.error('[Polymarket] getPrices error:', err.response?.data?.message || err.message);
      return {};
    }
  }

  /* ---------------------------------------------------------- */
  /*  Normalization                                              */
  /* ---------------------------------------------------------- */

  _normalize(raw, asset) {
    let outcomes, prices, tokenIds;
    try {
      outcomes = raw.outcomes ? JSON.parse(raw.outcomes) : [];
      prices = raw.outcomePrices ? JSON.parse(raw.outcomePrices) : [];
      tokenIds = raw.clobTokenIds ? JSON.parse(raw.clobTokenIds) : [];
    } catch (e) {
      console.error(`[Polymarket] failed to parse Gamma fields for ${raw.question || 'unknown'}:`, e.message);
      return null;
    }

    const yesIdx = outcomes.indexOf('Up') !== -1 ? outcomes.indexOf('Up') :
                   outcomes.indexOf('Yes') !== -1 ? outcomes.indexOf('Yes') : 0;
    const noIdx = outcomes.indexOf('Down') !== -1 ? outcomes.indexOf('Down') :
                  outcomes.indexOf('No') !== -1 ? outcomes.indexOf('No') : 1;

    const question = raw.question || raw.title || '';

    return {
      platform: 'polymarket',
      asset: asset.symbol,
      conditionId: raw.conditionId || raw.condition_id || raw.id || '',
      question,
      slug: raw.slug || '',
      outcomes,
      yesTokenId: tokenIds[yesIdx] || '',
      noTokenId: tokenIds[noIdx] || '',
      yesPrice: parseFloat(prices[yesIdx] || 0),
      noPrice: parseFloat(prices[noIdx] || 0),
      volume: parseFloat(raw.volume || raw.volumeNum || 0),
      liquidity: parseFloat(raw.liquidity || raw.liquidityNum || 0),
      endDate: raw.endDateIso || raw.end_date_iso || '',
      eventStartTime: raw.eventStartTime || '',
      active: raw.active !== false,
      closed: raw.closed === true,
      strike: null, // Filled later via Binance price at eventStartTime
      lastUpdated: new Date().toISOString(),
    };
  }

  /* ---------------------------------------------------------- */
  /*  Order placement                                            */
  /* ---------------------------------------------------------- */

  /**
   * Place FOK order at exact signal price — no split, no slippage.
   * If FOK fails, trade fails cleanly (no resting orders, no partial fills).
   */
  async placeOrder(order) {
    if (!this.connected) {
      console.error('[Polymarket] cannot place order — not connected');
      return null;
    }

    if (!this._clobClient) {
      console.error('[Polymarket] cannot place order — ClobClient not initialized (keys not derived)');
      return null;
    }

    if (!order.tokenId || order.tokenId.trim() === '') {
      console.error('[Polymarket] cannot place order — tokenId is empty');
      return null;
    }

    const orderType = order.orderType || 'FOK';
    console.log(`[Polymarket] placing ${orderType} order: ${order.side}@${order.price} x${order.size} tokenId=${order.tokenId.slice(0, 16)}...`);
    console.log(`[Polymarket] apiKey: ${this.apiKey ? this.apiKey.slice(0, 8) + '...' : 'MISSING'}, funder: ${this.funderAddress || 'none'}`);

    const resp = await this._postOrder(order.tokenId, order.price, order.size, order.side, orderType);

    if (resp !== null) {
      console.log(`[Polymarket] order SUCCESS (${orderType}): ${order.side}@${order.price} x${order.size} — ${JSON.stringify(resp).slice(0, 200)}`);
      return resp;
    }

    console.warn(`[Polymarket] ${orderType} FAILED — ${order.side}@${order.price} x${order.size}, not retrying`);
    return null;
  }

  /**
   * Post a single order via official CLOB v2 SDK create+post flow.
   * Keeping serialization + auth inside the SDK avoids manual wire drift
   * that can produce "invalid signature" responses.
   */
  async _postOrder(tokenId, price, size, side, orderType = 'FOK') {
    if (!this.wallet) {
      console.error('[Polymarket] cannot place order — no wallet');
      return null;
    }

    // Reset last-error so the hedge never reads stale data from a prior call.
    this._lastOrderError = null;

    try {
      const orderTypeEnum = orderType === 'FOK'
        ? OrderType.FOK
        : orderType === 'FAK'
          ? OrderType.FAK
          : orderType === 'GTD'
            ? OrderType.GTD
            : OrderType.GTC;
      const orderSide = side === 'BUY' ? Side.BUY : Side.SELL;
      const orderToSign = {
        tokenID: tokenId,
        price,
        size,
        side: orderSide,
      };
      const t0 = Date.now();
      const resp = await this._clobClient.createAndPostOrder(
        orderToSign,
        { tickSize: '0.01' },
        orderTypeEnum,
      );
      const postLatencyMs = Date.now() - t0;
      console.log(`[Polymarket] POST /order latency: ${postLatencyMs}ms (sdk)`);
      const data = resp?.data || resp;

      // CLOB returns 200 with {error: "..."} on validation failures
      if (data && data.error) {
        const errStr = typeof data.error === 'string' ? data.error : JSON.stringify(data.error);
        this._lastOrderError = this._classifyOrderError(errStr, 200);
        if (errStr.toLowerCase().includes('fully filled') || errStr.toLowerCase().includes('not filled')) {
          console.warn(`[Polymarket] FOK rejected (not enough liquidity at ${price}): ${errStr.slice(0, 200)}`);
          return null;
        }
        console.error(`[Polymarket] order FAILED (200 with error): ${errStr.slice(0, 300)}`);
        return null;
      }

      return data;
    } catch (err) {
      const status = err.response?.status || err.status || 'no-status';
      const data = err.response?.data ? JSON.stringify(err.response.data).slice(0, 300) : err.message;
      const location = err.response?.headers?.location || err.response?.headers?.Location;
      const reqUrl = err.config?.url || 'unknown';
      const reqMethod = err.config?.method || 'unknown';
      const errBody = err.response?.data?.error
        || (typeof err.response?.data === 'string' ? err.response.data : null)
        || err.message
        || 'unknown';
      this._lastOrderError = this._classifyOrderError(String(errBody), typeof status === 'number' ? status : null);
      console.error(`[Polymarket] order FAILED (HTTP ${status}): ${data}`);
      console.error(`[Polymarket] order FAILED context: ${reqMethod.toUpperCase()} ${reqUrl}${location ? ` → Location: ${location}` : ''}`);
      return null;
    }
  }

  /**
   * Parse a CLOB error string into structured hedge-actionable metadata.
   * Recognises:
   *   - "not enough balance / allowance: ... balance: 8720, order amount: 1000000"
   *     → type='balance', polyBalance / polyRequired in PUSD (6-decimal → float)
   *   - "fully filled" / "not filled" → type='liquidity'
   *   - anything else → type='other'
   */
  _classifyOrderError(errStr, httpStatus) {
    const msg = String(errStr || '').slice(0, 500);
    const lower = msg.toLowerCase();
    let type = 'other';
    let polyBalance = null;
    let polyRequired = null;

    if (lower.includes('not enough balance') || lower.includes('allowance')) {
      type = 'balance';
      const balMatch = msg.match(/balance:\s*(\d+)/i);
      const reqMatch = msg.match(/order\s*amount:\s*(\d+)/i);
      if (balMatch) polyBalance = parseInt(balMatch[1], 10) / 1e6;
      if (reqMatch) polyRequired = parseInt(reqMatch[1], 10) / 1e6;
    } else if (lower.includes('fully filled') || lower.includes('not filled')) {
      type = 'liquidity';
    }

    return {
      type,
      message: msg,
      polyBalance,
      polyRequired,
      httpStatus: httpStatus || null,
    };
  }

  /**
   * Split order into up to 2 lots based on orderbook depth.
   * Lot 1: available qty at original price
   * Lot 2: remaining qty at next ask (within 2¢ slippage)
   */
  async _splitOrder(tokenId, price, totalSize, side) {
    const MAX_SLIP = 0.02;

    try {
      // Fetch orderbook
      const axiosOpts = { timeout: 5000 };
      const bookResp = await axios.get(`${this.clobUrl}/book?token_id=${tokenId}`, axiosOpts);
      const book = bookResp.data;

      // For BUY orders, we look at asks (sellers); for SELL, we look at bids
      const levels = side === 'BUY'
        ? (book.asks || []).map(o => ({ price: parseFloat(o.price), size: parseFloat(o.size) })).sort((a, b) => a.price - b.price)
        : (book.bids || []).map(o => ({ price: parseFloat(o.price), size: parseFloat(o.size) })).sort((a, b) => b.price - a.price);

      if (levels.length === 0) {
        console.error('[Polymarket] split FAILED — orderbook empty');
        return null;
      }

      // Calculate available qty at original price and next level
      let qtyAtPrice = 0;
      let nextPrice = null;

      for (const level of levels) {
        if (side === 'BUY') {
          if (level.price <= price + 0.005) {
            qtyAtPrice += level.size;
          } else if (level.price <= price + MAX_SLIP + 0.005) {
            nextPrice = level.price;
            break;
          } else {
            break; // beyond max slippage
          }
        } else {
          if (level.price >= price - 0.005) {
            qtyAtPrice += level.size;
          } else if (level.price >= price - MAX_SLIP - 0.005) {
            nextPrice = level.price;
            break;
          } else {
            break;
          }
        }
      }

      console.log(`[Polymarket] orderbook: ${qtyAtPrice} available @${price}, next level @${nextPrice || 'none'}`);

      const slipNeeded = nextPrice ? Math.abs(nextPrice - price) : Infinity;

      if (slipNeeded > MAX_SLIP && qtyAtPrice < 1) {
        console.error(`[Polymarket] split FAILED — no liquidity within ${MAX_SLIP * 100}¢ slippage`);
        return null;
      }

      let filledTotal = 0;
      let lastResp = null;

      // Lot 1: fill at original price
      if (qtyAtPrice >= 1) {
        const qty1 = Math.min(Math.floor(qtyAtPrice), totalSize);
        console.log(`[Polymarket] lot 1: ${qty1} contracts @${price}`);
        const r1 = await this._postOrder(tokenId, price, qty1, side);
        if (r1) {
          filledTotal += qty1;
          lastResp = r1;
          console.log(`[Polymarket] lot 1 SUCCESS: ${qty1} filled`);
        } else {
          console.warn(`[Polymarket] lot 1 FAILED`);
        }
      }

      // Lot 2: fill remaining at next price level (within slippage)
      const remaining = totalSize - filledTotal;
      if (remaining >= 1 && nextPrice && slipNeeded <= MAX_SLIP) {
        console.log(`[Polymarket] lot 2: ${remaining} contracts @${nextPrice} (slip: ${(slipNeeded * 100).toFixed(1)}¢)`);
        const r2 = await this._postOrder(tokenId, nextPrice, remaining, side);
        if (r2) {
          filledTotal += remaining;
          lastResp = r2;
          console.log(`[Polymarket] lot 2 SUCCESS: ${remaining} filled`);
        } else {
          console.warn(`[Polymarket] lot 2 FAILED`);
        }
      }

      if (filledTotal === 0) {
        console.error(`[Polymarket] split order FAILED — 0 contracts filled`);
        return null;
      }

      console.log(`[Polymarket] split order DONE: ${filledTotal}/${totalSize} contracts filled`);
      return lastResp;
    } catch (err) {
      console.error(`[Polymarket] split order error: ${err.message}`);
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

module.exports = PolymarketConnector;
