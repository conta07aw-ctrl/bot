/**
 * Express + Socket.io server — serves the dashboard UI and REST API.
 */

const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const config = require('../config');
const { signToken, requireAuth } = require('../auth/middleware');
const userStore = require('../store/users');
const { checkAndApprove } = require('../services/usdcApproval');

function createServer(monitor, decision, capitalGuard, dispatcher, autoRedeemer, kalshi, polymarket) {
  const app = express();
  const server = http.createServer(app);
  const io = new Server(server, { cors: { origin: '*' } });

  app.use(express.json());

  /* ---------- Static files (dashboard) ---------- */

  app.use(express.static(path.join(__dirname, '..', 'public')));

  /* ---------- Public endpoints ---------- */

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', uptime: process.uptime() });
  });

  app.get('/prices', requireAuth, (_req, res) => {
    if (!monitor) return res.status(503).json({ error: 'monitor not ready' });
    res.json(monitor.getSnapshot());
  });

  app.get('/opportunities', requireAuth, (_req, res) => {
    if (!decision) return res.status(503).json({ error: 'decision engine not ready' });
    res.json({
      signals: decision.getSignals(),
      active: decision.getActiveSignals(),
    });
  });

  app.get('/trades', requireAuth, (_req, res) => {
    if (!dispatcher) return res.status(503).json({ error: 'dispatcher not ready' });
    res.json({
      state: dispatcher.getState(),
      trades: dispatcher.getTrades(),
      hedgeLog: dispatcher.getHedgeLog(),
      openPositions: dispatcher.getOpenPositions(),
    });
  });

  app.get('/balances', requireAuth, async (req, res) => {
    if (!capitalGuard) return res.json({ kalshi: null, polymarket: null });
    try {
      const keys = userStore.getKeys(req.user.username);
      const balances = await capitalGuard.getBalancesForUser(keys);
      const capState = capitalGuard.getState();
      res.json({
        ...balances,
        totalPnl: capState.totalPnl,
        tradeCount: capState.tradeCount,
      });
    } catch (err) {
      res.json({ kalshi: null, polymarket: null });
    }
  });

  /* ---------- Auth endpoints ---------- */

  app.post('/api/register', async (req, res) => {
    try {
      const { username, password, inviteCode } = req.body;
      if (!username || !password || password.length < 6) {
        return res.status(400).json({ error: 'Username and password (min 6 chars) required' });
      }
      if (!inviteCode) {
        return res.status(400).json({ error: 'Invite code required' });
      }
      const user = await userStore.createUser(username.toLowerCase().trim(), password, inviteCode);
      const token = signToken(user.username);
      res.json({ token, username: user.username, isAdmin: userStore.isAdmin(user.username) });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  app.post('/api/login', async (req, res) => {
    try {
      const { username, password } = req.body;
      const user = await userStore.authenticate(username?.toLowerCase().trim(), password);
      if (!user) return res.status(401).json({ error: 'Invalid credentials' });
      const token = signToken(user.username);
      res.json({ token, username: user.username, isAdmin: userStore.isAdmin(user.username) });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  /* ---------- Protected: API Keys ---------- */

  app.get('/api/keys/status', requireAuth, (req, res) => {
    const status = userStore.hasKeys(req.user.username);
    res.json(status);
  });

  app.post('/api/keys', requireAuth, async (req, res) => {
    try {
      userStore.saveKeys(req.user.username, req.body);
      const status = userStore.hasKeys(req.user.username);

      // Load keys into global connectors (if not already connected)
      // and trigger market rediscovery
      const keys = userStore.getKeys(req.user.username);
      if (kalshi && keys.kalshiApiKey && keys.kalshiPrivateKeyPem) {
        kalshi.loadUserKeys(keys).then(() => {
          if (monitor && kalshi.connected) {
            console.log('[Server] Kalshi keys loaded — re-discovering markets');
            monitor._discoverMarkets().catch(() => {});
          }
        }).catch(() => {});
      }
      if (polymarket && keys.polyApiKey) {
        polymarket.loadUserKeys(keys);
      }

      // Auto-check PUSD approval if Polymarket keys are set
      let approval = null;
      if (req.body.polyPrivateKey && req.body.polyFunderAddress) {
        try {
          approval = await checkAndApprove(req.body.polyPrivateKey, req.body.polyFunderAddress);
        } catch (err) {
          approval = { error: err.message };
        }
      }

      res.json({ ok: true, status, approval });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Protected: Settings ---------- */

  app.get('/api/settings', requireAuth, (req, res) => {
    const settings = userStore.getSettings(req.user.username);
    res.json(settings || {});
  });

  app.put('/api/settings', requireAuth, (req, res) => {
    try {
      const saved = userStore.saveSettings(req.user.username, req.body);

      // Apply settings to running engine
      if (capitalGuard) {
        capitalGuard.configure({
          entrySize: saved.entrySize,
          kalshiStopAt: saved.kalshiStopAt,
          polyStopAt: saved.polyStopAt,
        });
      }
      if (decision) {
        decision.configure({
          minRoiPct: saved.minRoiPct,
          maxStrikeDiff: saved.maxStrikeDiff,
        });
      }
      if (dispatcher && saved.tradeMode) {
        dispatcher.tradeMode = saved.tradeMode;
        console.log(`[Dispatcher] trade mode changed to: ${saved.tradeMode}`);
      }

      res.json(saved);
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Protected: Filters ---------- */

  app.get('/api/filters', requireAuth, (req, res) => {
    const filters = userStore.getFilters(req.user.username);
    res.json(filters || {});
  });

  app.put('/api/filters', requireAuth, (req, res) => {
    try {
      const saved = userStore.saveFilters(req.user.username, req.body);
      res.json(saved);
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Protected: WS Live Check toggle (admin only, global) ---------- */

  app.get('/api/system/livecheck', requireAuth, (req, res) => {
    if (!userStore.isAdmin(req.user.username)) return res.status(403).json({ error: 'Admin only' });
    res.json({ enabled: userStore.getSystemUseWsLivecheck() });
  });

  app.put('/api/system/livecheck', requireAuth, (req, res) => {
    if (!userStore.isAdmin(req.user.username)) return res.status(403).json({ error: 'Admin only' });
    try {
      const enabled = !!req.body.enabled;
      userStore.saveSystemUseWsLivecheck(enabled);
      if (dispatcher) {
        dispatcher.useWsLivecheck = enabled;
        console.log(`[Dispatcher] useWsLivecheck changed to: ${enabled} (admin toggle)`);
      }
      res.json({ ok: true, enabled });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Admin: WS max age for LIVE trades ---------- */

  app.get('/api/system/ws-max-age', requireAuth, (_req, res) => {
    res.json({ maxWsAgeForLive: dispatcher ? dispatcher.maxWsAgeForLive : 300 });
  });

  app.put('/api/system/ws-max-age', requireAuth, (req, res) => {
    if (!userStore.isAdmin(req.user.username)) return res.status(403).json({ error: 'Admin only' });
    try {
      const ms = parseInt(req.body.maxWsAgeForLive, 10);
      if (isNaN(ms) || ms < 0) return res.status(400).json({ error: 'maxWsAgeForLive must be >= 0 (0 = disabled)' });
      if (dispatcher) {
        dispatcher.maxWsAgeForLive = ms;
        console.log(`[Dispatcher] maxWsAgeForLive changed to: ${ms}ms (admin toggle)`);
      }
      res.json({ ok: true, maxWsAgeForLive: ms });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Public: Auto-Redeem log ---------- */

  app.get('/api/redeems', requireAuth, (_req, res) => {
    if (!autoRedeemer) return res.json([]);
    res.json(autoRedeemer.getLog());
  });

  /* ---------- Socket.io ---------- */

  io.on('connection', (socket) => {
    console.log('[WS] client connected:', socket.id);

    const onPrices = (data) => socket.emit('prices', data);
    const onOpps = (data) => socket.emit('opportunities', data);
    const onTrade = (data) => socket.emit('trade', data);

    if (monitor) {
      monitor.on('prices', onPrices);
      monitor.on('opportunities', onOpps);
      monitor.on('trade', onTrade);
    }

    socket.on('disconnect', () => {
      console.log('[WS] client disconnected:', socket.id);
      if (monitor) {
        monitor.removeListener('prices', onPrices);
        monitor.removeListener('opportunities', onOpps);
        monitor.removeListener('trade', onTrade);
      }
    });
  });

  return { app, server, io };
}

function startServer(monitor, decision, capitalGuard, dispatcher, autoRedeemer, kalshi, polymarket) {
  const { server, io } = createServer(monitor, decision, capitalGuard, dispatcher, autoRedeemer, kalshi, polymarket);
  server.listen(config.port, () => {
    console.log(`[Server] listening on :${config.port}`);
  });
  return { server, io };
}

module.exports = { createServer, startServer };
