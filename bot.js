require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { SocksProxyAgent } = require('socks-proxy-agent');

const LOG_FILE = path.join(__dirname, 'bot.log');

function log(level, message) {
  const ts = new Date().toISOString();
  const line = `[${ts}] [${level}] ${message}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

const CONFIG = {
  GATE_API_KEY:     process.env.GATE_API_KEY     || '',
  GATE_API_SECRET:  process.env.GATE_API_SECRET  || '',
  TELEGRAM_TOKEN:   process.env.TELEGRAM_TOKEN   || '',
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID || '',
  TELEGRAM_PROXY:   process.env.TELEGRAM_PROXY   || '',
  LEVERAGE:         parseInt(process.env.LEVERAGE || '10'),
  POLL_INTERVAL_MS: 2000,
  GATE_BASE:        'https://api.gateio.ws/api/v4',
  UPBIT_CRIX_URL:   'https://crix-static.upbit.com/v2/crix_master',
  ORDER_RETRIES:    10,
  ORDER_RETRY_MS:   300,
};

// ─── State ────────────────────────────────────────────────────────────────────
const knownCoins = new Map();
let initialized = false;
const processedTickers = new Map();
const TICKER_TTL_MS = 24 * 60 * 60 * 1000;
const STATE_FILE = path.join(__dirname, 'upbit-state.json');

// Завантажити стан з диску
function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      const now = Date.now();
      // Відновлюємо відомі монети
      if (data.knownCoins) {
        data.knownCoins.forEach(code => knownCoins.set(code, true));
        log('INFO', `State loaded: ${knownCoins.size} known coins from disk`);
      }
      // Відновлюємо оброблені тікери (тільки свіжі)
      if (data.processedTickers) {
        data.processedTickers.forEach(([ticker, ts]) => {
          if (now - ts < TICKER_TTL_MS) processedTickers.set(ticker, ts);
        });
      }
      initialized = knownCoins.size > 0;
    }
  } catch(e) {
    log('WARN', `Could not load state: ${e.message}`);
  }
}

// Зберегти стан на диск
function saveState() {
  try {
    const data = {
      knownCoins: [...knownCoins.keys()],
      processedTickers: [...processedTickers.entries()],
      savedAt: Date.now(),
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(data));
  } catch(e) {
    log('WARN', `Could not save state: ${e.message}`);
  }
}

// Зберігати стан кожні 30 сек
setInterval(saveState, 30000);

// ─── Кеш балансу (оптимізація 1) ─────────────────────────────────────────────
let cachedBalance = null;
let balanceUpdatedAt = 0;
const BALANCE_TTL_MS = 5000; // оновлювати кожні 5 сек

async function getCachedBalance() {
  const now = Date.now();
  if (cachedBalance && (now - balanceUpdatedAt) < BALANCE_TTL_MS) {
    return cachedBalance;
  }
  const account = await gateRequest('GET', '/futures/usdt/accounts', null, null);
  cachedBalance = parseFloat(account.available);
  balanceUpdatedAt = now;
  return cachedBalance;
}

// Оновлювати баланс у фоні кожні 5 сек
setInterval(async () => {
  try {
    const account = await gateRequest('GET', '/futures/usdt/accounts', null, null);
    cachedBalance = parseFloat(account.available);
    balanceUpdatedAt = Date.now();
  } catch(e) {}
}, BALANCE_TTL_MS);

setInterval(() => {
  const now = Date.now();
  for (const [t, ts] of processedTickers.entries()) {
    if (now - ts > TICKER_TTL_MS) processedTickers.delete(t);
  }
}, 60 * 60 * 1000);

// ─── Telegram асинхронно (оптимізація 3) ─────────────────────────────────────
function sendTelegram(text) {
  if (!CONFIG.TELEGRAM_TOKEN || !CONFIG.TELEGRAM_CHAT_ID) return;
  // Не чекаємо — відправляємо у фоні
  const agent = CONFIG.TELEGRAM_PROXY ? new SocksProxyAgent(CONFIG.TELEGRAM_PROXY) : undefined;
  axios.post(
    `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/sendMessage`,
    { chat_id: CONFIG.TELEGRAM_CHAT_ID, text },
    agent ? { httpsAgent: agent } : {}
  ).then(() => log('INFO', 'Telegram sent'))
   .catch(e => log('ERROR', `Telegram: ${e.message}`));
}

// ─── Gate.io ──────────────────────────────────────────────────────────────────
function sign(method, endpoint, query, body) {
  const ts = Math.floor(Date.now() / 1000).toString();
  const fullPath = '/api/v4' + endpoint;
  const bodyHash = crypto.createHash('sha512').update(body || '').digest('hex');
  const signString = `${method}\n${fullPath}\n${query || ''}\n${bodyHash}\n${ts}`;
  const sig = crypto.createHmac('sha512', CONFIG.GATE_API_SECRET).update(signString).digest('hex');
  return { ts, sig };
}

async function gateRequest(method, endpoint, query, data) {
  const url = CONFIG.GATE_BASE + endpoint;
  const qs = query ? new URLSearchParams(query).toString() : '';
  const body = data ? JSON.stringify(data) : '';
  const { ts, sig } = sign(method, endpoint, qs, body);
  const res = await axios({
    method,
    url: qs ? `${url}?${qs}` : url,
    headers: { 'KEY': CONFIG.GATE_API_KEY, 'SIGN': sig, 'Timestamp': ts, 'Content-Type': 'application/json' },
    data: data || undefined,
    timeout: 5000,
  });
  return res.data;
}

// ─── Паралельна перевірка контракту і балансу (оптимізація 4) ────────────────
async function contractExists(ticker) {
  const contract = `${ticker}_USDT`;
  try {
    const res = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts/${contract}`, { timeout: 5000 });
    return {
      exists: true, contract,
      markPrice: parseFloat(res.data.mark_price),
      quanto: parseFloat(res.data.quanto_multiplier || '1'),
    };
  } catch (e) {
    if (e.response?.status === 404) return { exists: false, contract };
    throw e;
  }
}

async function openOrderWithRetry(contract, size) {
  for (let attempt = 1; attempt <= CONFIG.ORDER_RETRIES; attempt++) {
    try {
      const order = await gateRequest('POST', '/futures/usdt/orders', null, {
        contract, size, price: '0', tif: 'ioc', text: 't-listing',
      });
      log('INFO', `Order opened on attempt ${attempt}`);
      return order;
    } catch (e) {
      const msg = e.response?.data?.message || e.message;
      log('WARN', `Attempt ${attempt}/${CONFIG.ORDER_RETRIES}: ${msg}`);
      if (attempt < CONFIG.ORDER_RETRIES) {
        await new Promise(r => setTimeout(r, CONFIG.ORDER_RETRY_MS));
      } else {
        throw e;
      }
    }
  }
}

async function handleNewListing(ticker, seenAt) {
  if (processedTickers.has(ticker)) return;
  processedTickers.set(ticker, Date.now());

  log('INFO', `NEW LISTING [Upbit]: ${ticker}`);

  // Оптимізація 4: паралельно перевіряємо контракт і баланс
  const [contractData, available] = await Promise.all([
    contractExists(ticker),
    getCachedBalance(),
  ]);

  if (!contractData.exists) {
    log('WARN', `No Gate.io contract for ${ticker}`);
    sendTelegram(`[Upbit] Немає контракту для ${ticker} на Gate.io`);
    return;
  }

  const { contract, markPrice, quanto } = contractData;

  // Плече встановлюємо паралельно з розрахунком розміру (не чекаємо)
  gateRequest('POST', `/futures/usdt/positions/${contract}/leverage`,
    { leverage: '0', cross_leverage_limit: String(CONFIG.LEVERAGE) }, null
  ).catch(e => log('WARN', `Leverage: ${e.response?.data?.message || e.message}`));

  const useMargin = available * 0.9;
  const size = Math.max(1, Math.floor((useMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  const posValue = (size * markPrice * quanto).toFixed(2);

  log('INFO', `Opening: ${contract} size=${size} price=${markPrice} value=$${posValue}`);

  let order;
  try {
    order = await openOrderWithRetry(contract, size);
  } catch (e) {
    log('ERROR', `All retries failed: ${e.response?.data?.message || e.message}`);
    sendTelegram(`ПОМИЛКА [Upbit] ${contract}: ${e.response?.data?.message || e.message}`);
    return;
  }

  const entryPrice = parseFloat(order.fill_price) || markPrice;
  const elapsedSec = ((Date.now() - seenAt) / 1000).toFixed(2);

  // Оновлюємо кеш балансу після відкриття
  cachedBalance = null;

  sendTelegram(
    `ПОЗИЦІЯ ВІДКРИТА 🟢\n` +
    `─────────────────────\n` +
    `📌 Монета:     ${ticker}\n` +
    `🔗 Джерело:    UPBIT\n` +
    `📄 Контракт:   ${contract}\n` +
    `💵 Ціна входу: ${entryPrice} USDT\n` +
    `📊 Розмір:     $${posValue}\n` +
    `⚡️ Плече:      ${CONFIG.LEVERAGE}x\n` +
    `⏱ Швидкість:   ${elapsedSec} сек\n` +
    `─────────────────────`
  );

  log('INFO', `Opened ${contract} entry=${entryPrice} speed=${elapsedSec}s`);
}

// ─── Upbit тік ────────────────────────────────────────────────────────────────
let isRunning = false;

async function tick() {
  if (isRunning) return;
  isRunning = true;
  try {
    const res = await axios.get(CONFIG.UPBIT_CRIX_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json', 'Origin': 'https://upbit.com' },
      timeout: 10000,
    });
    const coins = Array.isArray(res.data) ? res.data : [];
    const seenAt = Date.now();

    if (!initialized) {
      for (const coin of coins) {
        const code = coin.code || coin.baseCurrencyCode;
        if (code) knownCoins.set(code, coin);
      }
      log('INFO', `Initialized with ${knownCoins.size} coins from Upbit`);
      initialized = true;
      saveState();
      return;
    }

    for (const coin of coins) {
      const code = coin.baseCurrencyCode || coin.code;
      if (!code) continue;
      if (coin.quoteCurrencyCode !== 'KRW') continue;
      if (coin.marketState !== 'ACTIVE') continue;
      if (!knownCoins.has(coin.code || code)) {
        knownCoins.set(coin.code || code, coin);
        await handleNewListing(code, seenAt);
      }
    }
  } catch (e) {
    log('ERROR', `Tick: ${e.message}`);
  } finally {
    isRunning = false;
  }
}

async function main() {
  log('INFO', '══════════════════════════════════════');
  log('INFO', ' Upbit Listing Bot v3 — starting up');
  log('INFO', '══════════════════════════════════════');
  log('INFO', `LEV=${CONFIG.LEVERAGE}x | Optimized: cache+async+parallel`);

  // Завантажуємо збережений стан
  loadState();

  sendTelegram(
    'Upbit Listing Bot v3 запущен\n' +
    'Оптимізації: кеш балансу + async TG + паралельні запити\n' +
    `Плече: ${CONFIG.LEVERAGE}x`
  );

  await tick();
  setInterval(tick, CONFIG.POLL_INTERVAL_MS);
}

process.on('uncaughtException', async (e) => {
  log('ERROR', `Uncaught: ${e.message}`);
  sendTelegram(`UPBIT БОТ ВПАВ!\n${e.message}\nПерезапускається...`);
  setTimeout(() => process.exit(1), 1000);
});

process.on('unhandledRejection', async (e) => {
  log('ERROR', `Unhandled: ${e?.message || e}`);
  sendTelegram(`UPBIT БОТ ВПАВ!\n${e?.message || e}\nПерезапускається...`);
  setTimeout(() => process.exit(1), 1000);
});

main().catch(e => {
  log('ERROR', `Fatal: ${e.message}`);
  sendTelegram(`UPBIT БОТ ВПАВ!\n${e.message}`);
  setTimeout(() => process.exit(1), 1000);
});
