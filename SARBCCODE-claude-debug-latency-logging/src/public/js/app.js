/**
 * SARBCCODE Dashboard — auth, real-time Socket.io, per-asset filters, capital protection.
 */

/* ---------------------------------------------------------- */
/*  Auth State                                                 */
/* ---------------------------------------------------------- */

let authToken = localStorage.getItem('sarbccode_token') || null;
let authUser = localStorage.getItem('sarbccode_user') || null;
let isAdmin = localStorage.getItem('sarbccode_admin') === 'true';
let isRegisterMode = false;

function authHeaders() {
  return authToken ? { Authorization: `Bearer ${authToken}` } : {};
}

async function apiFetch(url, opts = {}) {
  opts.headers = { ...opts.headers, ...authHeaders() };
  if (opts.body && typeof opts.body === 'object') {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(opts.body);
  }
  const res = await fetch(url, opts);
  if (res.status === 401) { logout(); throw new Error('Session expired'); }
  return res;
}

/* ---------------------------------------------------------- */
/*  Auth UI                                                    */
/* ---------------------------------------------------------- */

function showLoginModal() { document.getElementById('login-modal').classList.remove('hidden'); }
function hideLoginModal() { document.getElementById('login-modal').classList.add('hidden'); }

function setAuthMode(register) {
  isRegisterMode = register;
  document.getElementById('auth-title').textContent = register ? 'Register' : 'Login';
  document.getElementById('auth-subtitle').textContent = register
    ? 'Create an account (invite code required)'
    : 'Enter your credentials to access the dashboard';
  document.getElementById('auth-submit').textContent = register ? 'Register' : 'Login';
  document.getElementById('auth-toggle').textContent = register
    ? 'Already have an account? Login'
    : "Don't have an account? Register";
  document.getElementById('auth-error').style.display = 'none';
  document.getElementById('invite-group').style.display = register ? '' : 'none';
}

function showAuthError(msg) {
  const el = document.getElementById('auth-error');
  el.textContent = msg;
  el.style.display = 'block';
}

async function handleAuth() {
  const username = document.getElementById('auth-username').value.trim();
  const password = document.getElementById('auth-password').value;

  if (!username || !password) return showAuthError('Username and password required');
  if (isRegisterMode && password.length < 6) return showAuthError('Password must be at least 6 characters');

  const body = { username, password };

  if (isRegisterMode) {
    const inviteCode = document.getElementById('auth-invite').value.trim();
    if (!inviteCode) return showAuthError('Invite code required');
    body.inviteCode = inviteCode;
  }

  const endpoint = isRegisterMode ? '/api/register' : '/api/login';

  try {
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok) return showAuthError(data.error || 'Request failed');

    authToken = data.token;
    authUser = data.username;
    isAdmin = !!data.isAdmin;
    localStorage.setItem('sarbccode_token', authToken);
    localStorage.setItem('sarbccode_user', authUser);
    localStorage.setItem('sarbccode_admin', String(isAdmin));

    hideLoginModal();
    onLoggedIn();
  } catch (err) {
    showAuthError('Network error');
  }
}

function logout() {
  authToken = null;
  authUser = null;
  isAdmin = false;
  localStorage.removeItem('sarbccode_token');
  localStorage.removeItem('sarbccode_user');
  localStorage.removeItem('sarbccode_admin');
  document.getElementById('user-display').textContent = '';
  document.getElementById('logout-btn').style.display = 'none';
  showLoginModal();
}

function onLoggedIn() {
  document.getElementById('user-display').textContent = authUser;
  document.getElementById('logout-btn').style.display = '';
  loadSettings();
  loadFilters();
  loadKeyStatus();
  if (isAdmin) {
    document.getElementById('livecheck-panel').style.display = '';
    loadLivecheckStatus();
  }
}

document.getElementById('auth-submit').addEventListener('click', handleAuth);
document.getElementById('auth-password').addEventListener('keydown', (e) => { if (e.key === 'Enter') handleAuth(); });
document.getElementById('auth-toggle').addEventListener('click', () => setAuthMode(!isRegisterMode));
document.getElementById('logout-btn').addEventListener('click', logout);

/* ---------------------------------------------------------- */
/*  Tab Navigation                                             */
/* ---------------------------------------------------------- */

document.querySelectorAll('.nav-tab').forEach((tab) => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.nav-tab').forEach((t) => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach((c) => c.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById(`tab-${tab.dataset.tab}`).classList.add('active');
  });
});

/* ---------------------------------------------------------- */
/*  State                                                      */
/* ---------------------------------------------------------- */

const ASSETS = ['BTC', 'ETH'];
const ASSET_NAMES = { BTC: 'Bitcoin', ETH: 'Ethereum', SOL: 'Solana', XRP: 'XRP' };
const state = { prices: {}, opportunities: [], trades: [], capital: null, dispatcher: null, lastPoll: null };

/* ---------------------------------------------------------- */
/*  Socket.io                                                  */
/* ---------------------------------------------------------- */

const socket = io();

socket.on('connect', () => {
  document.getElementById('ws-dot').className = 'status-dot connected';
  document.getElementById('ws-text').textContent = 'Connected';
  fetchAll();
});

socket.on('disconnect', () => {
  document.getElementById('ws-dot').className = 'status-dot disconnected';
  document.getElementById('ws-text').textContent = 'Disconnected';
});

socket.on('prices', (data) => {
  if (data.asset && data.kalshi) {
    state.prices[data.asset] = { kalshi: data.kalshi, polymarket: data.polymarket, updatedAt: data.timestamp };
    renderPriceCard(data.asset);
    updatePollTime(data.timestamp);
  }
});

socket.on('opportunities', (opps) => { state.opportunities = opps || []; renderOpportunities(); });
socket.on('trade', (trade) => { if (trade) { state.trades.push(trade); renderTrades(); fetchTrades(); } });

/* ---------------------------------------------------------- */
/*  REST fetches                                               */
/* ---------------------------------------------------------- */

async function fetchAll() { await Promise.all([fetchPrices(), fetchOpportunities(), fetchTrades(), fetchBalances()]); }

async function fetchPrices() {
  try {
    const res = await apiFetch('/prices');
    const data = await res.json();
    state.prices = data.prices || {};
    state.lastPoll = data.lastPoll;
    renderAllPrices();
    updatePollTime(data.lastPoll);
  } catch (_) {}
}

async function fetchOpportunities() {
  try {
    const res = await apiFetch('/opportunities');
    const data = await res.json();
    state.opportunities = data.signals || [];
    renderOpportunities();
  } catch (_) {}
}

async function fetchTrades() {
  try {
    const res = await apiFetch('/trades');
    const data = await res.json();
    state.trades = data.trades || [];
    state.dispatcher = data.state || null;
    renderTrades();
    renderStatusStrip();
  } catch (_) {}
}

async function fetchBalances() {
  try {
    const res = await apiFetch('/balances');
    const data = await res.json();
    if (!state.capital) state.capital = {};
    state.capital.balances = data;
    state.capital.totalPnl = data.totalPnl ?? state.capital.totalPnl ?? 0;
    state.capital.tradeCount = data.tradeCount ?? state.capital.tradeCount ?? 0;
    renderStatusStrip();
  } catch (_) {}
}

// Refresh balances every 30s
setInterval(fetchBalances, 30_000);

/* ---------------------------------------------------------- */
/*  Settings load/save                                         */
/* ---------------------------------------------------------- */

async function loadSettings() {
  try {
    const res = await apiFetch('/api/settings');
    const s = await res.json();
    if (s.tradeMode) document.getElementById('set-tradeMode').value = s.tradeMode;
    if (s.kalshiStopAt != null) document.getElementById('set-kalshiStopAt').value = s.kalshiStopAt;
    if (s.polyStopAt != null) document.getElementById('set-polyStopAt').value = s.polyStopAt;
    if (s.earlyExitThreshold != null) document.getElementById('set-earlyExitThreshold').value = s.earlyExitThreshold;
    document.getElementById('set-telegramEnabled').checked = !!s.telegramEnabled;
    if (s.telegramBotToken) document.getElementById('set-telegramBotToken').value = s.telegramBotToken;
    if (s.telegramChatId) document.getElementById('set-telegramChatId').value = s.telegramChatId;
  } catch (_) {}
}

async function saveSettings() {
  const settings = {
    tradeMode: document.getElementById('set-tradeMode').value,
    kalshiStopAt: parseFloat(document.getElementById('set-kalshiStopAt').value) || 0,
    polyStopAt: parseFloat(document.getElementById('set-polyStopAt').value) || 0,
    earlyExitThreshold: parseFloat(document.getElementById('set-earlyExitThreshold').value) || 0.20,
    telegramEnabled: document.getElementById('set-telegramEnabled').checked,
    telegramBotToken: document.getElementById('set-telegramBotToken').value.trim(),
    telegramChatId: document.getElementById('set-telegramChatId').value.trim(),
  };
  try {
    await apiFetch('/api/settings', { method: 'PUT', body: settings });
    flashMsg('settings-msg');
  } catch (_) {}
}

document.getElementById('save-settings-btn').addEventListener('click', saveSettings);

/* ---------------------------------------------------------- */
/*  Per-asset Filters load/save                                */
/* ---------------------------------------------------------- */

async function loadFilters() {
  try {
    const res = await apiFetch('/api/filters');
    const filters = await res.json();
    renderFiltersForm(filters);
  } catch (_) {
    renderFiltersForm({});
  }
}

function renderFiltersForm(filters) {
  const wrap = document.getElementById('filters-form');
  wrap.innerHTML = '';

  // Header row
  const header = document.createElement('div');
  header.style.cssText = 'display:grid; grid-template-columns:50px 50px 1fr 1fr 1fr; gap:8px; padding:6px 0; font-size:11px; color:var(--text-secondary); border-bottom:1px solid var(--border)';
  header.innerHTML = '<span>Asset</span><span>Active</span><span>Entry $ (exact)</span><span>Min ROI %</span><span>Max Strike Diff $</span>';
  wrap.appendChild(header);

  for (const asset of ASSETS) {
    const f = (filters[asset] && typeof filters[asset] === 'object')
      ? filters[asset]
      : { enabled: filters[asset] !== false, entrySize: 10, minRoiPct: 1.0, maxStrikeDiff: 5.0 };

    const row = document.createElement('div');
    row.style.cssText = 'display:grid; grid-template-columns:50px 50px 1fr 1fr 1fr; gap:8px; align-items:center; padding:8px 0; border-bottom:1px solid var(--border)';
    row.innerHTML = `
      <span style="font-weight:700">${asset}</span>
      <input type="checkbox" id="flt-${asset}-on" ${f.enabled !== false ? 'checked' : ''} style="accent-color:var(--green); width:18px; height:18px">
      <input type="number" id="flt-${asset}-entry" value="${f.entrySize || 10}" step="1" min="1" style="padding:4px 8px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;color:var(--text-primary);font-size:12px">
      <input type="number" id="flt-${asset}-roi" value="${f.minRoiPct || 1.0}" step="0.5" min="0" style="padding:4px 8px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;color:var(--text-primary);font-size:12px">
      <input type="number" id="flt-${asset}-div" value="${f.maxStrikeDiff || 5.0}" step="1" min="0" style="padding:4px 8px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;color:var(--text-primary);font-size:12px">
    `;
    wrap.appendChild(row);
  }
}

async function saveFilters() {
  const filters = {};
  for (const asset of ASSETS) {
    filters[asset] = {
      enabled: document.getElementById(`flt-${asset}-on`).checked,
      entrySize: parseFloat(document.getElementById(`flt-${asset}-entry`).value) || 10,
      minRoiPct: parseFloat(document.getElementById(`flt-${asset}-roi`).value) || 1.0,
      maxStrikeDiff: parseFloat(document.getElementById(`flt-${asset}-div`).value) || 5.0,
    };
  }
  try {
    await apiFetch('/api/filters', { method: 'PUT', body: filters });
    flashMsg('filters-msg');
  } catch (_) {}
}

document.getElementById('save-filters-btn').addEventListener('click', saveFilters);

/* ---------------------------------------------------------- */
/*  API Keys                                                   */
/* ---------------------------------------------------------- */

async function loadKeyStatus() {
  try {
    const res = await apiFetch('/api/keys/status');
    const status = await res.json();
    setKeyBadge('kalshi-key-status', status.kalshi);
    setKeyBadge('poly-key-status', status.polymarket);
  } catch (_) {}
}

function setKeyBadge(id, configured) {
  const el = document.getElementById(id);
  el.textContent = configured ? 'Configured' : 'Not set';
  el.className = `key-status ${configured ? 'ok' : 'missing'}`;
}

async function saveKeys() {
  const keys = {
    kalshiApiKey: document.getElementById('key-kalshiApiKey').value.trim(),
    kalshiPrivateKeyPem: document.getElementById('key-kalshiPrivateKeyPem').value.trim(),
    polyApiKey: document.getElementById('key-polyApiKey').value.trim(),
    polyApiSecret: document.getElementById('key-polyApiSecret').value.trim(),
    polyPassphrase: document.getElementById('key-polyPassphrase').value.trim(),
    polyPrivateKey: document.getElementById('key-polyPrivateKey').value.trim(),
    polyFunderAddress: document.getElementById('key-polyFunderAddress').value.trim(),
  };
  try {
    const res = await apiFetch('/api/keys', { method: 'POST', body: keys });
    const data = await res.json();
    if (data.status) {
      setKeyBadge('kalshi-key-status', data.status.kalshi);
      setKeyBadge('poly-key-status', data.status.polymarket);
    }
    if (data.approval) {
      const a = data.approval;
      if (a.error) {
        flashMsg('keys-msg', `Saved! (approval check error: ${a.error})`);
      } else {
        flashMsg('keys-msg', `Saved & encrypted! PUSD: ${a.pusd ? 'OK' : 'pending'}, CTF: ${a.ctf ? 'OK' : 'pending'}`);
      }
    } else {
      flashMsg('keys-msg');
    }
    // Clear sensitive fields
    document.getElementById('key-kalshiApiKey').value = '';
    document.getElementById('key-kalshiPrivateKeyPem').value = '';
    document.getElementById('key-polyApiKey').value = '';
    document.getElementById('key-polyApiSecret').value = '';
    document.getElementById('key-polyPassphrase').value = '';
    document.getElementById('key-polyPrivateKey').value = '';
  } catch (_) {}
}

document.getElementById('save-keys-btn').addEventListener('click', saveKeys);

/* ---------------------------------------------------------- */
/*  WS Live Check toggle (admin only, global)                  */
/* ---------------------------------------------------------- */

let livecheckEnabled = false;

function _renderLivecheckUi() {
  const statusEl = document.getElementById('livecheck-status');
  const btnEl = document.getElementById('toggle-livecheck-btn');
  if (livecheckEnabled) {
    statusEl.textContent = 'Enabled';
    statusEl.className = 'key-status ok';
    btnEl.textContent = 'Disable';
    btnEl.className = 'btn';
  } else {
    statusEl.textContent = 'Disabled';
    statusEl.className = 'key-status missing';
    btnEl.textContent = 'Enable';
    btnEl.className = 'btn btn-primary';
  }
}

async function loadLivecheckStatus() {
  try {
    const res = await apiFetch('/api/system/livecheck');
    const data = await res.json();
    livecheckEnabled = !!data.enabled;
    _renderLivecheckUi();
  } catch (_) {}
}

async function toggleLivecheck() {
  try {
    const next = !livecheckEnabled;
    await apiFetch('/api/system/livecheck', { method: 'PUT', body: { enabled: next } });
    livecheckEnabled = next;
    _renderLivecheckUi();
    flashMsg('livecheck-msg');
  } catch (_) {}
}

document.getElementById('toggle-livecheck-btn').addEventListener('click', toggleLivecheck);

/* ---------------------------------------------------------- */
/*  Render: Status Strip                                       */
/* ---------------------------------------------------------- */

function renderStatusStrip() {
  const cap = state.capital;
  const disp = state.dispatcher;

  if (cap) {
    const kBal = cap.balances?.kalshi;
    const pBal = cap.balances?.polymarket;
    document.getElementById('bal-kalshi').textContent = kBal !== null && kBal !== undefined ? `$${kBal.toFixed(2)}` : '$--';
    document.getElementById('bal-poly').textContent = pBal !== null && pBal !== undefined ? `$${pBal.toFixed(2)}` : '$--';

    const pnlEl = document.getElementById('bk-pnl');
    const pnl = cap.totalPnl || 0;
    pnlEl.textContent = `${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`;
    pnlEl.className = `value ${pnl >= 0 ? 'positive' : 'negative'}`;

    document.getElementById('bk-trades').textContent = cap.tradeCount || 0;
  }

  if (disp) {
    const modeEl = document.getElementById('bk-mode');
    modeEl.textContent = disp.tradeMode;
    modeEl.className = `value ${disp.tradeMode === 'live' ? 'mode-live' : 'mode-dry'}`;
  }
}

/* ---------------------------------------------------------- */
/*  Render: Price Cards                                        */
/* ---------------------------------------------------------- */

function renderAllPrices() {
  const grid = document.getElementById('price-grid');
  grid.innerHTML = '';
  for (const symbol of ASSETS) renderPriceCard(symbol);
}

function renderPriceCard(symbol) {
  const grid = document.getElementById('price-grid');
  const data = state.prices[symbol];

  let card = document.getElementById(`card-${symbol}`);
  if (!card) {
    card = document.createElement('div');
    card.className = 'price-card';
    card.id = `card-${symbol}`;
    grid.appendChild(card);
  }

  const kalshi = data?.kalshi?.[0];
  const poly = data?.polymarket?.[0];

  const kYes = kalshi?.yesAsk > 0 ? kalshi.yesAsk.toFixed(3) : (kalshi?.lastPrice > 0 ? kalshi.lastPrice.toFixed(3) : '--');
  const kNo = kalshi?.noAsk > 0 ? kalshi.noAsk.toFixed(3) : '--';
  const pUp = poly?.yesPrice > 0 ? poly.yesPrice.toFixed(3) : '--';
  const pDown = poly?.noPrice > 0 ? poly.noPrice.toFixed(3) : '--';
  const pVol = poly ? `$${formatNum(poly.volume)}` : '--';
  const pLiq = poly ? `$${formatNum(poly.liquidity)}` : '--';

  const question = poly?.question || '';
  const timeMatch = question.match(/(\d{1,2}:\d{2}[AP]M)-(\d{1,2}:\d{2}[AP]M)/);
  const timeStr = timeMatch ? `${timeMatch[1]} - ${timeMatch[2]}` : '';

  // Strike / target prices from both platforms
  const kStrikeVal = kalshi?.strike > 0 ? kalshi.strike : null;
  const pStrikeVal = poly?.strike > 0 ? poly.strike : null;
  // Show decimal places based on price magnitude:
  // BTC/ETH ($1000+) → 2 decimals, SOL ($10-999) → 4 decimals, XRP (<$10) → 5 decimals
  const fmtStrike = (v) => {
    const n = Number(v);
    const decimals = n >= 1000 ? 2 : n >= 10 ? 4 : 5;
    return `$${n.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`;
  };

  let strikeDisplay = '';
  if (kStrikeVal && pStrikeVal) {
    const diff = Math.abs(kStrikeVal - pStrikeVal);
    const diffDecimals = diff >= 1 ? 2 : diff >= 0.01 ? 4 : 5;
    strikeDisplay = `K:${fmtStrike(kStrikeVal)} P:${fmtStrike(pStrikeVal)} (diff $${diff.toFixed(diffDecimals)})`;
  } else if (kStrikeVal) {
    strikeDisplay = fmtStrike(kStrikeVal);
  } else if (pStrikeVal) {
    strikeDisplay = fmtStrike(pStrikeVal);
  }

  card.innerHTML = `
    <div class="asset-header">
      <span class="asset-name">${symbol}${strikeDisplay ? `&nbsp;&nbsp;<span style="font-weight:400;font-size:12px;color:var(--text-secondary)">${strikeDisplay}</span>` : ''}</span>
      <span class="asset-symbol">${timeStr || '15 MIN'}</span>
    </div>
    <div class="platform-row">
      <span class="platform-label kalshi">Kalshi</span>
      <div class="price-values"><span class="price-up">Yes ${kYes}</span><span class="price-down">No ${kNo}</span></div>
    </div>
    <div class="platform-row">
      <span class="platform-label poly">Polymarket</span>
      <div class="price-values"><span class="price-up">Up ${pUp}</span><span class="price-down">Down ${pDown}</span></div>
    </div>
    <div class="volume-info">Vol: ${pVol} &middot; Liq: ${pLiq}</div>
  `;
}

/* ---------------------------------------------------------- */
/*  Render: Opportunities                                      */
/* ---------------------------------------------------------- */

function renderOpportunities() {
  const wrap = document.getElementById('opp-table-wrap');
  const badge = document.getElementById('opp-count');
  const opps = state.opportunities;

  badge.textContent = `${opps.length} signal${opps.length !== 1 ? 's' : ''}`;
  badge.className = opps.length > 0 ? 'panel-badge green' : 'panel-badge orange';

  if (opps.length === 0) {
    wrap.innerHTML = '<div class="empty-state">Scanning for cross-platform arbitrage...</div>';
    return;
  }

  wrap.innerHTML = `<table><thead><tr><th>Asset</th><th>Leg</th><th>Kalshi</th><th>Poly</th><th>Cost</th><th>ROI</th><th>Net</th><th>Size</th><th>Action</th></tr></thead><tbody>
    ${opps.map(o => `<tr>
      <td><strong>${o.asset}</strong></td><td>Leg ${o.leg}</td>
      <td>${o.kalshiSide} @ ${o.kalshiPrice}</td><td>${o.polySide} @ ${o.polyPrice}</td>
      <td>$${o.totalCost}</td><td style="color:var(--green)">${o.roiPct}%</td>
      <td style="color:var(--green)">$${o.netProfit}</td>
      <td>${o.sizing ? `${o.sizing.units} ($${o.sizing.betSize})` : '--'}</td>
      <td><span class="tag ${o.execute ? 'buy' : 'sell'}">${o.execute ? 'EXECUTE' : 'WATCH'}</span></td>
    </tr>`).join('')}
  </tbody></table>`;
}

/* ---------------------------------------------------------- */
/*  Render: Trade History                                      */
/* ---------------------------------------------------------- */

function renderTrades() {
  const wrap = document.getElementById('trade-table-wrap');
  const badge = document.getElementById('trade-count');
  const trades = state.trades;

  badge.textContent = `${trades.length} trade${trades.length !== 1 ? 's' : ''}`;

  if (trades.length === 0) {
    wrap.innerHTML = '<div class="empty-state">No trades executed yet</div>';
    return;
  }

  const sorted = [...trades].reverse();
  wrap.innerHTML = `<table><thead><tr><th>Time</th><th>Asset</th><th>Leg</th><th>Kalshi</th><th>Poly</th><th>Units</th><th>ROI</th><th>Status</th></tr></thead><tbody>
    ${sorted.map(t => {
      const status = t.dryRun ? 'dry-run' : t.hedged ? 'hedged' : t.blocked ? 'blocked' : t.success ? 'filled' : t.partial ? 'partial' : 'failed';
      const time = new Date(t.timestamp).toLocaleTimeString();
      return `<tr>
        <td>${time}</td><td><strong>${t.asset}</strong></td><td>Leg ${t.leg}</td>
        <td>${t.kalshiSide} @ ${t.kalshiPrice}</td><td>${t.polySide} @ ${t.polyPrice}</td>
        <td>${t.units}</td><td style="color:var(--green)">${(() => { const c = t.kalshiPrice + t.polyPrice; const wf = Math.max((1-t.kalshiPrice)*(t.kalshiFee||0),(1-t.polyPrice)*(t.polyFee||0)); return c > 0 ? ((1-c-wf)/c*100).toFixed(1) : '0.0'; })()}%</td>
        <td><span class="tag ${status}">${status.toUpperCase()}</span></td>
      </tr>`;
    }).join('')}
  </tbody></table>`;
}

/* ---------------------------------------------------------- */
/*  Helpers                                                    */
/* ---------------------------------------------------------- */

function formatNum(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
}

function updatePollTime(ts) {
  if (!ts) return;
  document.getElementById('poll-time').textContent = `Updated: ${new Date(ts).toLocaleTimeString()}`;
}

function flashMsg(id, text) {
  const el = document.getElementById(id);
  if (text) el.textContent = text;
  el.style.display = '';
  setTimeout(() => { el.style.display = 'none'; }, 3000);
}

/* ---------------------------------------------------------- */
/*  Init                                                       */
/* ---------------------------------------------------------- */

if (authToken) { hideLoginModal(); onLoggedIn(); }
else { showLoginModal(); }

renderAllPrices();
