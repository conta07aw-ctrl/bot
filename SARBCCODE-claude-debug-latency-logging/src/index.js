/**
 * SARBCCODE — Entry point
 * Boots all subsystems: connectors → capital guard → decision engine → dispatcher → monitor → server.
 */

const { startServer } = require('./api/server');
const KalshiConnector = require('./connectors/kalshi');
const PolymarketConnector = require('./connectors/polymarket');
const CapitalGuard = require('./engine/capitalGuard');
const DecisionEngine = require('./engine/decisionEngine');
const Dispatcher = require('./execution/dispatcher');
const Monitor = require('./engine/monitor');
const RtdsClient = require('./services/rtdsClient');
const AutoRedeemer = require('./services/autoRedeemer');
const TelegramNotifier = require('./services/telegram');
const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');
const userStore = require('./store/users');
const config = require('./config');

/**
 * Event loop lag monitor.
 *
 * Measures how long setImmediate takes to fire after being scheduled. If the
 * loop is healthy, lag is <5ms. If Node is blocked (GC pause, big sync work,
 * long JSON parse, etc.) lag balloons. This explains anomalies where BOTH
 * legs of a trade are slow simultaneously without any network issue — the
 * process itself was frozen.
 *
 * Samples every 1s. Warns when lag exceeds 100ms (roughly the threshold
 * where it could meaningfully delay an order). Silent below that.
 */
function startEventLoopLagMonitor() {
  setInterval(() => {
    const start = Date.now();
    setImmediate(() => {
      const lag = Date.now() - start;
      if (lag > 100) {
        // Heap stats help diagnose GC-pause lag: if heapUsed is near heapTotal
        // and spikes correlate with drops, V8 is doing mark-and-sweep.
        const mem = process.memoryUsage();
        const heapUsedMB = Math.round(mem.heapUsed / 1024 / 1024);
        const heapTotalMB = Math.round(mem.heapTotal / 1024 / 1024);
        const rssMB = Math.round(mem.rss / 1024 / 1024);
        const externalMB = Math.round(mem.external / 1024 / 1024);
        console.warn(`[EventLoop] lag: ${lag}ms | heap: ${heapUsedMB}/${heapTotalMB}MB rss: ${rssMB}MB ext: ${externalMB}MB`);
      }
    });
  }, 1_000).unref();
}

async function main() {
  // Force stdout/stderr to non-blocking mode so console.log doesn't stall
  // the event loop on slow-disk writes. PM2 redirects stdout to a file fd,
  // and Node treats file-fd writes as synchronous. Measured on this VPS:
  // each sync 4k write = ~2.9ms (DO block storage). A burst of 200 log
  // lines = ~580ms event loop block — exactly matches the lag pattern.
  //
  // Trade-off: under extreme bursts the kernel may return EAGAIN and Node
  // will drop the write silently. For a trading bot, trading latency is
  // far more important than log fidelity — Telegram alerts cover the
  // critical-path notifications anyway.
  try {
    if (process.stdout._handle && typeof process.stdout._handle.setBlocking === 'function') {
      process.stdout._handle.setBlocking(false);
    }
    if (process.stderr._handle && typeof process.stderr._handle.setBlocking === 'function') {
      process.stderr._handle.setBlocking(false);
    }
  } catch (_) { /* best effort — if it fails, logs stay synchronous */ }

  console.log('========================================');
  console.log(' SARBCCODE — Simultaneous Asymmetric ARB');
  console.log('========================================');

  if (process.env.POLY_PROXY_URL) {
    console.log('[Boot] using Polymarket proxy:', process.env.POLY_PROXY_URL.replace(/:([^:@]+)@/, ':***@'));
  }

  startEventLoopLagMonitor();

  // 1. Initialize connectors (start without keys — will be loaded from user config)
  const kalshi = new KalshiConnector();
  const polymarket = new PolymarketConnector();

  // Try to load keys from the first existing user (auto-start if keys are configured)
  const users = userStore.listUsers();
  if (users.length > 0) {
    const keys = userStore.getKeys(users[0]);
    if (keys?.kalshiApiKey && keys?.kalshiPrivateKeyPem) {
      kalshi.apiKey = keys.kalshiApiKey;
      kalshi.privateKey = keys.kalshiPrivateKeyPem;
      console.log(`[Boot] loaded Kalshi keys from user: ${users[0]}`);
    }
    if (keys?.polyApiKey) {
      polymarket.loadUserKeys(keys);
      console.log(`[Boot] loaded Polymarket keys from user: ${users[0]}`);
    }
  }

  await kalshi.connect();
  await polymarket.connect();

  // Auto-derive Polymarket API keys from wallet (replaces Builder Codes)
  if (polymarket.wallet && polymarket.connected) {
    console.log('[Boot] deriving Polymarket API keys from wallet...');
    await polymarket.deriveApiKeys();
  }

  // 2. Initialize capital guard (real balance checking + stop thresholds)
  const capitalGuard = new CapitalGuard(kalshi, polymarket);

  // Load settings from first user if available
  if (users.length > 0) {
    const settings = userStore.getSettings(users[0]);
    if (settings) {
      capitalGuard.configure({
        entrySize: settings.entrySize,
        kalshiStopAt: settings.kalshiStopAt,
        polyStopAt: settings.polyStopAt,
      });
    }
  }

  // Start background balance refresh BEFORE the dispatcher can call canTrade().
  // This primes the cache non-blockingly and installs the 60s refresh timer,
  // so the dispatcher critical path is pure synchronous cache reads.
  capitalGuard.start();

  // 3. Initialize decision engine (arbitrage detection)
  const decision = new DecisionEngine(capitalGuard);

  // 4. Initialize Telegram notifier
  const telegram = new TelegramNotifier();

  // 5. Initialize dispatcher (parallel execution + emergency hedge)
  const dispatcher = new Dispatcher(kalshi, polymarket, capitalGuard);

  // Load trade mode from first user
  if (users.length > 0) {
    const settings = userStore.getSettings(users[0]);
    if (settings?.tradeMode) {
      dispatcher.tradeMode = settings.tradeMode;
    }
  }

  // Load global WS live-check toggle (admin-configured, default true).
  // When false, dispatcher uses REST Promise.all for step 1 instead of the WS cache.
  dispatcher.useWsLivecheck = userStore.getSystemUseWsLivecheck();
  console.log(`[Dispatcher] mode: ${dispatcher.tradeMode} | emergency hedge: enabled | wsLivecheck: ${dispatcher.useWsLivecheck}`);

  // Start early exit monitor — checks open positions every 2s for profitable exits
  dispatcher.startEarlyExitMonitor();

  // 6. Start RTDS client (must connect before Monitor so strikes are ready)
  const rtdsClient = new RtdsClient();
  rtdsClient.connect();

  // Wait for RTDS to deliver at least some prices (max 15s)
  // RTDS delivers BTC within 1-2s, others may take longer
  console.log('[Boot] waiting for RTDS prices...');
  await new Promise((resolve) => {
    let resolved = false;
    const done = () => { if (!resolved) { resolved = true; resolve(); } };

    const check = setInterval(() => {
      const symbols = ['BTC', 'ETH'];
      const ready = symbols.filter((s) => rtdsClient.getLatestPrice(s) !== null);
      if (ready.length === symbols.length) {
        clearInterval(check);
        done();
      }
    }, 500);

    setTimeout(() => { clearInterval(check); done(); }, 15_000);
  });

  const allPrices = ['BTC', 'ETH'].map((s) => {
    const p = rtdsClient.getLatestPrice(s);
    return p ? `${s}=$${p.price.toFixed(2)}` : `${s}=waiting`;
  });
  console.log(`[Boot] RTDS prices: ${allPrices.join(', ')}`);

  // 7. Start global monitor (event-driven via WebSocket)
  const monitor = new Monitor(kalshi, polymarket, decision, dispatcher, rtdsClient);
  dispatcher.monitor = monitor;
  await monitor.start();

  // 8. Wire up Telegram notifications
  monitor.on('trade', (trade) => telegram.notifyTrade(trade));

  // 9. Start auto-redeemer (24/7 background)
  const autoRedeemer = new AutoRedeemer();
  autoRedeemer.start(polymarket);

  // 10. Start API / dashboard server (pass connectors for key loading)
  startServer(monitor, decision, capitalGuard, dispatcher, autoRedeemer, kalshi, polymarket);

  // Fetch initial balances for first user
  if (users.length > 0) {
    const keys = userStore.getKeys(users[0]);
    capitalGuard.getBalancesForUser(keys).then((b) => {
      const kBal = b.kalshi !== null ? `$${b.kalshi.toFixed(2)}` : 'N/A';
      const pBal = b.polymarket !== null ? `$${b.polymarket.toFixed(2)}` : 'N/A';
      console.log(`[CapitalGuard] balances — Kalshi: ${kBal}, Polymarket: ${pBal}`);
    }).catch(() => {});
  }
}

main().catch((err) => {
  console.error('[FATAL]', err);
  process.exit(1);
});
