/**
 * User store — file-based storage with encrypted API keys.
 *
 * Each user has:
 *   - login credentials (bcrypt-hashed password)
 *   - encrypted platform API keys (AES-256-GCM)
 *   - per-asset filter settings
 *   - trade settings
 *
 * Invite codes: required for registration, single-use, generated via CLI.
 * Data stored at data/users.json + data/invites.json (gitignored).
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const config = require('../config');

const DATA_DIR = path.join(__dirname, '..', '..', 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const INVITES_FILE = path.join(DATA_DIR, 'invites.json');
const SYSTEM_FILE = path.join(DATA_DIR, 'system.json');
const ALGO = 'aes-256-gcm';

function _getEncKey() {
  const secret = config.dashboard.secret || 'sarbccode-default-key-change-me';
  return crypto.scryptSync(secret, 'sarbccode-salt', 32);
}

function _encrypt(text) {
  if (!text) return '';
  const key = _getEncKey();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(ALGO, key, iv);
  let enc = cipher.update(text, 'utf8', 'hex');
  enc += cipher.final('hex');
  const tag = cipher.getAuthTag().toString('hex');
  return `${iv.toString('hex')}:${tag}:${enc}`;
}

function _decrypt(data) {
  if (!data || !data.includes(':')) return '';
  const [ivHex, tagHex, enc] = data.split(':');
  const key = _getEncKey();
  const decipher = crypto.createDecipheriv(ALGO, key, Buffer.from(ivHex, 'hex'));
  decipher.setAuthTag(Buffer.from(tagHex, 'hex'));
  let dec = decipher.update(enc, 'hex', 'utf8');
  dec += decipher.final('utf8');
  return dec;
}

function _ensureDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function _load() {
  if (!fs.existsSync(USERS_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8')); }
  catch { return {}; }
}

function _save(users) {
  _ensureDir();
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
}

function _loadInvites() {
  if (!fs.existsSync(INVITES_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(INVITES_FILE, 'utf8')); }
  catch { return {}; }
}

function _saveInvites(invites) {
  _ensureDir();
  fs.writeFileSync(INVITES_FILE, JSON.stringify(invites, null, 2));
}

/* ---------------------------------------------------------- */
/*  Invite Codes                                               */
/* ---------------------------------------------------------- */

/**
 * Generate an invite code.
 * @param {number|null} expiresInDays - null = lifetime, number = expires in N days
 * @returns {{ code: string, expiresAt: string|null }}
 */
function createInvite(expiresInDays = null) {
  const invites = _loadInvites();
  const code = crypto.randomBytes(8).toString('hex').toUpperCase(); // 16-char code

  const expiresAt = expiresInDays
    ? new Date(Date.now() + expiresInDays * 86400000).toISOString()
    : null;

  invites[code] = {
    createdAt: new Date().toISOString(),
    expiresAt,
    used: false,
    usedBy: null,
  };

  _saveInvites(invites);
  return { code, expiresAt };
}

/**
 * Validate and consume an invite code.
 * Returns true if valid, throws if invalid/expired/used.
 */
function useInvite(code) {
  if (!code) throw new Error('Invite code required');

  const invites = _loadInvites();
  const invite = invites[code.toUpperCase()];

  if (!invite) throw new Error('Invalid invite code');
  if (invite.used) throw new Error('Invite code already used');
  if (invite.expiresAt && new Date(invite.expiresAt) < new Date()) {
    throw new Error('Invite code expired');
  }

  return true; // valid, will be consumed in createUser
}

function _consumeInvite(code, username) {
  const invites = _loadInvites();
  const key = code.toUpperCase();
  if (invites[key]) {
    invites[key].used = true;
    invites[key].usedBy = username;
    invites[key].usedAt = new Date().toISOString();
    _saveInvites(invites);
  }
}

/**
 * List all invite codes (for admin CLI).
 */
function listInvites() {
  return _loadInvites();
}

/* ---------------------------------------------------------- */
/*  Users                                                      */
/* ---------------------------------------------------------- */

// Default per-asset filter
const DEFAULT_ASSET_FILTER = {
  enabled: true,
  entrySize: 10,           // exact $ per trade for this asset
  minRoiPct: 1.0,          // min ROI % for this asset
  maxStrikeDiff: 5.0,      // max strike price difference in $ between platforms
};

/**
 * Register a new user (requires valid invite code).
 */
async function createUser(username, password, inviteCode) {
  // Validate invite first
  useInvite(inviteCode);

  const users = _load();
  if (users[username]) throw new Error('User already exists');

  const hash = await bcrypt.hash(password, 10);
  users[username] = {
    passwordHash: hash,
    createdAt: new Date().toISOString(),
    inviteCode: inviteCode.toUpperCase(),
    keys: {
      kalshiApiKey: '',
      kalshiPrivateKeyPem: '',
      polyApiKey: '',
      polyApiSecret: '',
      polyPassphrase: '',
      polyPrivateKey: '',
      polyFunderAddress: '',
    },
    settings: {
      tradeMode: 'dry-run',
      kalshiStopAt: 0,      // 0 = no stop, >0 = stop if balance below this
      polyStopAt: 0,
      telegramEnabled: false,
      telegramBotToken: '',
      telegramChatId: '',
    },
    filters: {
      BTC: { ...DEFAULT_ASSET_FILTER },
      ETH: { ...DEFAULT_ASSET_FILTER },
    },
  };

  _save(users);
  _consumeInvite(inviteCode, username);

  return { username, createdAt: users[username].createdAt };
}

async function authenticate(username, password) {
  const users = _load();
  const user = users[username];
  if (!user) return null;

  const match = await bcrypt.compare(password, user.passwordHash);
  if (!match) return null;

  return { username, createdAt: user.createdAt };
}

function saveKeys(username, keys) {
  const users = _load();
  if (!users[username]) throw new Error('User not found');

  users[username].keys = {
    kalshiApiKey: _encrypt(keys.kalshiApiKey || ''),
    kalshiPrivateKeyPem: _encrypt(keys.kalshiPrivateKeyPem || ''),
    polyApiKey: _encrypt(keys.polyApiKey || ''),
    polyApiSecret: _encrypt(keys.polyApiSecret || ''),
    polyPassphrase: _encrypt(keys.polyPassphrase || ''),
    polyPrivateKey: _encrypt(keys.polyPrivateKey || ''),
    polyFunderAddress: keys.polyFunderAddress || '',
  };

  _save(users);
}

function getKeys(username) {
  const users = _load();
  const user = users[username];
  if (!user) return null;

  const k = user.keys || {};
  return {
    kalshiApiKey: _decrypt(k.kalshiApiKey),
    kalshiPrivateKeyPem: _decrypt(k.kalshiPrivateKeyPem),
    polyApiKey: _decrypt(k.polyApiKey),
    polyApiSecret: _decrypt(k.polyApiSecret),
    polyPassphrase: _decrypt(k.polyPassphrase),
    polyPrivateKey: _decrypt(k.polyPrivateKey),
    polyFunderAddress: k.polyFunderAddress || '',
  };
}

function hasKeys(username) {
  const users = _load();
  const user = users[username];
  if (!user) return { kalshi: false, polymarket: false };

  const k = user.keys || {};
  return {
    kalshi: !!(k.kalshiApiKey && k.kalshiPrivateKeyPem),
    polymarket: !!(k.polyApiKey && k.polyApiSecret && k.polyPassphrase),
  };
}

function saveSettings(username, settings) {
  const users = _load();
  if (!users[username]) throw new Error('User not found');
  users[username].settings = { ...users[username].settings, ...settings };
  _save(users);
  return users[username].settings;
}

function getSettings(username) {
  const users = _load();
  return users[username]?.settings || null;
}

function saveFilters(username, filters) {
  const users = _load();
  if (!users[username]) throw new Error('User not found');
  users[username].filters = { ...users[username].filters, ...filters };
  _save(users);
  return users[username].filters;
}

function getFilters(username) {
  const users = _load();
  return users[username]?.filters || null;
}

function listUsers() {
  const users = _load();
  return Object.keys(users);
}

/**
 * Admin = first registered user.
 */
function isAdmin(username) {
  const users = _load();
  const firstUser = Object.keys(users)[0];
  return firstUser === username;
}

/* ---------------------------------------------------------- */
/*  System Config (global, admin-only)                         */
/* ---------------------------------------------------------- */

function _loadSystem() {
  if (!fs.existsSync(SYSTEM_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(SYSTEM_FILE, 'utf8')); }
  catch { return {}; }
}

function _saveSystem(data) {
  _ensureDir();
  fs.writeFileSync(SYSTEM_FILE, JSON.stringify(data, null, 2));
}

/**
 * Global WS live-check toggle. When true, the dispatcher reads top-of-book
 * from the in-memory WS cache (~20ms). When false, it falls back to REST
 * (~150-1800ms). Default true. Admin-only.
 */
function saveSystemUseWsLivecheck(enabled) {
  const sys = _loadSystem();
  sys.useWsLivecheck = !!enabled;
  _saveSystem(sys);
}

function getSystemUseWsLivecheck() {
  const sys = _loadSystem();
  // Default to true when never set, so new installs get the fast path.
  return sys.useWsLivecheck === undefined ? true : !!sys.useWsLivecheck;
}

function getSystemProxy() {
  const sys = _loadSystem();
  return sys.proxyUrl || process.env.POLY_PROXY_URL || '';
}

module.exports = {
  createUser,
  authenticate,
  saveKeys,
  getKeys,
  hasKeys,
  listUsers,
  isAdmin,
  saveSettings,
  getSettings,
  saveFilters,
  getFilters,
  createInvite,
  useInvite,
  listInvites,
  saveSystemUseWsLivecheck,
  getSystemUseWsLivecheck,
  getSystemProxy,
};
