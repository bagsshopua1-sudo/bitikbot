require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const https = require('https');
const WebSocket = require('ws');
const { SocksProxyAgent } = require('socks-proxy-agent');

const LOG_FILE = path.join(__dirname, 'bot.log');

function log(level, message) {
  const ts = new Date().toISOString();
  const line = `[${ts}] [${level}] ${message}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

// ─── HTTP Keep-Alive agent ────────────────────────────────────────────────────
const keepAliveAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 10,
  timeout: 30000,
});

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
  ORDER_RETRIES:    30,
  ORDER_RETRY_MS:   1000,
};

// ─── WS кеш цін від Gate.io ───────────────────────────────────────────────────
const priceCache = new Map(); // contract → { markPrice, quanto }
let priceWs = null;

function startPriceWS() {
  priceWs = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');

  priceWs.on('open', () => {
    log('INFO', '[PriceWS] Connected to Gate.io');
    // Підписуємось на всі тікери
    priceWs.send(JSON.stringify({
      time: Math.floor(Date.now() / 1000),
      channel: 'futures.tickers',
      event: 'subscribe',
      payload: ['!all'],
    }));
  });

  priceWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.channel === 'futures.tickers' && msg.event === 'update') {
        const tickers = Array.isArray(msg.result) ? msg.result : [msg.result];
        for (const t of tickers) {
          if (t.contract && t.mark_price) {
            priceCache.set(t.contract, {
              markPrice: parseFloat(t.mark_price),
              quanto: parseFloat(t.quanto_multiplier || '1'),
            });
          }
        }
      }
    } catch(e) {}
  });

  priceWs.on('error', e => log('ERROR', `[PriceWS] ${e.message}`));
  priceWs.on('close', () => {
    log('WARN', '[PriceWS] Disconnected — reconnecting in 2s...');
    setTimeout(startPriceWS, 2000);
  });

  // Ping кожні 20 сек
  setInterval(() => {
    if (priceWs && priceWs.readyState === WebSocket.OPEN) {
      priceWs.send(JSON.stringify({ time: Math.floor(Date.now() / 1000), channel: 'futures.ping' }));
    }
  }, 20000);
}

// ─── WS кеш балансу від Gate.io ──────────────────────────────────────────────
let wsBalance = null;
let balanceWs = null;

function signWs(channel, event, ts) {
  const str = `channel=${channel}&event=${event}&time=${ts}`;
  return crypto.createHmac('sha512', CONFIG.GATE_API_SECRET).update(str).digest('hex');
}

function startBalanceWS() {
  balanceWs = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');

  balanceWs.on('open', () => {
    log('INFO', '[BalanceWS] Connected to Gate.io');
    const ts = Math.floor(Date.now() / 1000);
    balanceWs.send(JSON.stringify({
      time: ts,
      channel: 'futures.balances',
      event: 'subscribe',
      payload: [],
      auth: { method: 'api_key', KEY: CONFIG.GATE_API_KEY, SIGN: signWs('futures.balances', 'subscribe', ts) }
    }));
  });

  balanceWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.channel === 'futures.balances' && msg.event === 'update') {
        const result = Array.isArray(msg.result) ? msg.result[0] : msg.result;
        if (result && result.available) {
          wsBalance = parseFloat(result.available);
          log('INFO', `[BalanceWS] Balance updated: ${wsBalance}`);
        }
      }
    } catch(e) {}
  });

  balanceWs.on('error', e => log('ERROR', `[BalanceWS] ${e.message}`));
  balanceWs.on('close', () => {
    log('WARN', '[BalanceWS] Disconnected — reconnecting in 2s...');
    setTimeout(startBalanceWS, 2000);
  });

  setInterval(() => {
    if (balanceWs && balanceWs.readyState === WebSocket.OPEN) {
      balanceWs.send(JSON.stringify({ time: Math.floor(Date.now() / 1000), channel: 'futures.ping' }));
    }
  }, 20000);
}

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
    httpsAgent: keepAliveAgent,
  });
  return res.data;
}

// ─── Кеш контрактів Gate.io ───────────────────────────────────────────────────
const contractsCache = new Map();
let contractsCacheUpdatedAt = 0;
const CONTRACTS_TTL = 5 * 60 * 1000;

async function updateContractsCache() {
  try {
    const res = await axios.get('https://api.gateio.ws/api/v4/futures/usdt/contracts', {
      timeout: 10000,
    });
    contractsCache.clear();
    for (const c of res.data) {
      contractsCache.set(c.name, {
        markPrice: parseFloat(c.mark_price),
        quanto: parseFloat(c.quanto_multiplier || '1'),
      });
    }
    contractsCacheUpdatedAt = Date.now();
    log('INFO', `Contracts cache: ${contractsCache.size} contracts`);
  } catch(e) {
    log('ERROR', `Contracts cache failed: ${e.message}`);
  }
}

// ─── Паралельна перевірка контракту і балансу (оптимізація 4) ────────────────
async function contractExists(ticker) {
  const contract = `${ticker}_USDT`;

  // Спочатку перевіряємо WS кеш цін (~0ms)
  if (priceCache.has(contract)) {
    const cached = priceCache.get(contract);
    log('INFO', `[PriceCache] ${contract}: ${cached.markPrice}`);
    return { exists: true, contract, markPrice: cached.markPrice, quanto: cached.quanto };
  }

  if (Date.now() - contractsCacheUpdatedAt > CONTRACTS_TTL) {
    await updateContractsCache();
  }

  if (contractsCache.has(contract)) {
    try {
      const res = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts/${contract}`, { timeout: 3000, httpsAgent: keepAliveAgent });
      return {
        exists: true, contract,
        markPrice: parseFloat(res.data.mark_price),
        quanto: parseFloat(res.data.quanto_multiplier || '1'),
      };
    } catch(e) {
      const data = contractsCache.get(contract);
      return { exists: true, contract, markPrice: data.markPrice, quanto: data.quanto };
    }
  }

  return { exists: false, contract };
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
  const [contractData, freshAccount] = await Promise.all([
    contractExists(ticker),
    wsBalance ? Promise.resolve({ available: wsBalance }) : gateRequest('GET', '/futures/usdt/accounts', null, null),
  ]);

  const available = wsBalance || parseFloat(freshAccount.available);

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

  // Підбираємо розмір так щоб маржа точно влізала в 75% балансу
  // Враховуємо що Gate.io додає ~15% буфер до маржі
  const targetMargin = available * 0.65;
  let size = Math.max(1, Math.floor((targetMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  
  log('INFO', `Size calc: available=${available} target_margin=${targetMargin.toFixed(2)} size=${size} est_margin=${(size*markPrice*quanto/CONFIG.LEVERAGE).toFixed(2)}`);
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

// ─── Upbit анонси через проксі ────────────────────────────────────────────────
const seenNoticeIds = new Set();
let noticesInitialized = false;

const LISTING_KEYWORDS = ['추가', '신규 상장', '거래 지원', '디지털 자산 추가', '신규 거래지원', '거래지원 안내'];
const SKIP_KEYWORDS = ['입출금', '점검', '이벤트', '중단', '종료', '폐지', '유의'];

function extractTickerFromTitle(title) {
  const matches = title.match(/\(([A-Z]{2,10})\)/g) || [];
  return matches.map(m => m.replace(/[()]/g, ''));
}

let noticeRunning = false;

async function tickNotices() {
  if (noticeRunning) return;
  noticeRunning = true;
  try {
    const agent = CONFIG.TELEGRAM_PROXY ? new SocksProxyAgent(CONFIG.TELEGRAM_PROXY) : undefined;
    const res = await axios.get(
      'https://api-manager.upbit.com/api/v1/announcements?os=moweb&page=1&per_page=20&category=all',
      {
        httpsAgent: agent,
        headers: {
          'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15',
          'Accept': 'application/json',
          'Accept-Language': 'ko-KR,ko;q=0.9',
          'Origin': 'https://upbit.com',
          'Referer': 'https://upbit.com/service-center/notice',
        },
        timeout: 10000,
      }
    );

    const notices = res.data?.data?.notices || [];
    const seenAt = Date.now();

    if (!noticesInitialized) {
      notices.forEach(n => seenNoticeIds.add(n.id));
      log('INFO', `[Upbit Notices] Initialized with ${seenNoticeIds.size} notices`);
      noticesInitialized = true;
      return;
    }

    for (const notice of notices) {
      if (seenNoticeIds.has(notice.id)) continue;
      seenNoticeIds.add(notice.id);

      const title = notice.title || '';
      log('INFO', `[Upbit Notice] New: ${title}`);

      // Пропускаємо не лістинги
      if (SKIP_KEYWORDS.some(w => title.includes(w))) {
        log('INFO', `[Upbit Notice] Skip: ${title}`);
        continue;
      }

      // Перевіряємо чи це лістинг
      if (!LISTING_KEYWORDS.some(w => title.includes(w))) {
        sendTelegram(`🟡 Upbit анонс (перевір):\n${title}`);
        continue;
      }

      // Витягуємо тікери
      const tickers = extractTickerFromTitle(title);
      if (tickers.length === 0) {
        sendTelegram(`🟡 Upbit лістинг без тікера:\n${title}`);
        continue;
      }

      log('INFO', `[Upbit Notice] LISTING! Tickers: ${tickers.join(', ')}`);

      for (const ticker of tickers) {
        await handleNewListing(ticker, seenAt);
      }
    }
  } catch (e) {
    log('ERROR', `[Upbit Notices] Tick: ${e.message}`);
  } finally {
    noticeRunning = false;
  }
}

// ─── CoinListing WebSocket (миттєві лістинги Upbit) ──────────────────────────
const WS_URL = 'wss://seoul.coinlisting.pro/listings?key=ilyak-2c3dbb';

function startCoinListingWS() {
  const ws = new (require('ws'))(WS_URL);

  ws.on('open', () => {
    log('INFO', '[CoinListing] Connected to seoul.coinlisting.pro');
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'connection') {
        log('INFO', `[CoinListing] Auth OK | tier=${msg.tier} | delay=${msg.delay_ms}ms`);
        return;
      }
      
      log('INFO', `[CoinListing] MSG: ${JSON.stringify(msg)}`);
      
      // Формат: { source, title, coins: ["BABY"], detected_at_iso }
      if (msg.source === 'UPBIT' && msg.coins && msg.coins.length > 0) {
        const seenAt = Date.now();
        for (const ticker of msg.coins) {
          if (ticker && ticker !== '***') {
            log('INFO', `[CoinListing] UPBIT LISTING: ${ticker}`);
            await handleNewListing(ticker, seenAt);
          }
        }
      }
    } catch(e) {
      log('ERROR', `[CoinListing] Parse error: ${e.message}`);
    }
  });

  ws.on('error', (e) => {
    log('ERROR', `[CoinListing] Error: ${e.message}`);
  });

  ws.on('close', (code, reason) => {
    log('WARN', `[CoinListing] Disconnected: ${code} — reconnecting in 3s...`);
    sendTelegram(`⚠️ CoinListing WS відключився! Перепідключаємось...`);
    setTimeout(startCoinListingWS, 3000);
  });
}

let isRunning = false;
let lastChecksum = null;

async function tick() {
  if (isRunning) return;
  isRunning = true;
  try {
    // Спочатку перевіряємо тільки checksum — маленький запит
    const tsRes = await axios.get('https://crix-static.upbit.com/v2/crix_master_timestamp', {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Origin': 'https://upbit.com' },
      timeout: 5000,
    });

    const checksum = tsRes.data?.checksum;

    // Якщо checksum не змінився — пропускаємо
    if (lastChecksum && lastChecksum === checksum) {
      isRunning = false;
      return;
    }

    lastChecksum = checksum;

    // Checksum змінився → тягнемо повний список
    const res = await axios.get(CONFIG.UPBIT_CRIX_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json', 'Origin': 'https://upbit.com' },
      timeout: 30000,
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

  // Запускаємо WS кеш цін і балансу
  startPriceWS();
  startBalanceWS();

  // Завантажуємо кеш контрактів
  await updateContractsCache();
  setInterval(updateContractsCache, CONTRACTS_TTL);

  // CoinListing WebSocket — міттєві лістинги Upbit
  startCoinListingWS();

  await tick();
  // Щогодинний heartbeat
  setInterval(() => {
    const now = new Date().toISOString();
    log('INFO', 'Heartbeat — bot is alive');
    sendTelegram(`✅ Upbit бот живий\n${now}`);
  }, 60 * 60 * 1000);

  setInterval(tick, CONFIG.POLL_INTERVAL_MS);

  // Анонси Upbit через проксі (резерв)
  await tickNotices();
  setInterval(tickNotices, 60000);
}

process.on('uncaughtException', (e) => {
  log('ERROR', `Uncaught: ${e.message}`);
  sendTelegram(`⚠️ UPBIT БОТ: помилка\n${e.message}\nПродовжує працювати...`);
});

process.on('unhandledRejection', (e) => {
  log('ERROR', `Unhandled: ${e?.message || e}`);
  sendTelegram(`⚠️ UPBIT БОТ: помилка\n${e?.message || e}\nПродовжує працювати...`);
});

main().catch(e => {
  log('ERROR', `Fatal: ${e.message}`);
  sendTelegram(`UPBIT БОТ ВПАВ!\n${e.message}`);
  setTimeout(() => process.exit(1), 1000);
});
