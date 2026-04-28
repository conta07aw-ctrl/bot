/**
 * Central configuration — loads .env and exports typed settings.
 *
 * API keys are NOT stored here — each user configures their own
 * keys in the dashboard (encrypted in data/users.json).
 */

require('dotenv').config();

module.exports = {
  port: parseInt(process.env.PORT, 10) || 3001,
  env: process.env.NODE_ENV || 'development',

  kalshi: {
    baseUrl: process.env.KALSHI_BASE_URL || 'https://api.elections.kalshi.com',
    wsUrl: process.env.KALSHI_WS_URL || 'wss://api.elections.kalshi.com/trade-api/ws/v2',
  },

  polymarket: {
    baseUrl: process.env.POLY_BASE_URL || 'https://clob.polymarket.com',
    proxyUrl: process.env.POLY_PROXY_URL || '',
  },

  tradeMode: process.env.TRADE_MODE || 'dry-run',

  dashboard: {
    secret: process.env.DASHBOARD_SECRET || '',
  },

  fees: {
    kalshi: parseFloat(process.env.KALSHI_FEE_PCT || '0.07'),
    polymarket: parseFloat(process.env.POLY_FEE_PCT || '0.02'),
    minRoiPct: parseFloat(process.env.MIN_ROI_PCT || '1.0'),
  },

  // Telegram is per-user (configured in dashboard settings, not .env)
};
