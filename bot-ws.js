require('dotenv').config();
const axios  = require('axios');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const WebSocket = require('ws');
const { SocksProxyAgent } = require('socks-proxy-agent');

const LOG_FILE = path.join(__dirname, 'bot-ws.log');

function log(level, message) {
  const ts   = new Date().toISOString();
  const line = `[${ts}] [${level}] ${message}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

// ─── Config ───────────────────────────────────────────────────────────────────
const CONFIG = {
  GATE_API_KEY:    process.env.GATE_API_KEY    || '',
  GATE_API_SECRET: process.env.GATE_API_SECRET || '',
  TELEGRAM_TOKEN:  process.env.TELEGRAM_TOKEN  || '',
  TELEGRAM_CHAT_ID:process.env.TELEGRAM_CHAT_ID|| '',
  TELEGRAM_PROXY:  process.env.TELEGRAM_PROXY  || '',
  LEVERAGE:        parseInt(process.env.LEVERAGE || '10'),
  POLL_INTERVAL_MS:2000,
  GATE_BASE:       'https://api.gateio.ws/api/v4',
  GATE_WS_URL:     'wss://fx-ws.gateio.ws/v4/ws/usdt',
  COINLISTING_URL: 'wss://seoul.coinlisting.pro/listings?key=ilyak-2c3dbb',
  UPBIT_CRIX_URL:  'https://crix-static.upbit.com/v2/crix_master',
  ORDER_TIMEOUT_MS: 5000,  // чекаємо відповідь від WS ордера
};

// ─── State ────────────────────────────────────────────────────────────────────
const knownCoins       = new Map();
const processedTickers = new Map();
const TICKER_TTL_MS    = 24 * 60 * 60 * 1000;
const STATE_FILE       = path.join(__dirname, 'upbit-state.json');
let initialized        = false;
let lastChecksum       = null;
let isRunning          = false;

// ─── State persistence ────────────────────────────────────────────────────────
function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    const now  = Date.now();
    (data.knownCoins || []).forEach(c => knownCoins.set(c, true));
    (data.processedTickers || []).forEach(([t, ts]) => {
      if (now - ts < TICKER_TTL_MS) processedTickers.set(t, ts);
    });
    initialized = knownCoins.size > 0;
    log('INFO', `State loaded: ${knownCoins.size} known coins`);
  } catch(e) {
    log('WARN', `State load failed: ${e.message}`);
  }
}

function saveState() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify({
      knownCoins: [...knownCoins.keys()],
      processedTickers: [...processedTickers.entries()],
      savedAt: Date.now(),
    }));
  } catch(e) {
    log('WARN', `State save failed: ${e.message}`);
  }
}

setInterval(saveState, 30000);
setInterval(() => {
  const now = Date.now();
  for (const [t, ts] of processedTickers.entries()) {
    if (now - ts > TICKER_TTL_MS) processedTickers.delete(t);
  }
}, 60 * 60 * 1000);

// ─── Telegram ─────────────────────────────────────────────────────────────────
function sendTelegram(text) {
  if (!CONFIG.TELEGRAM_TOKEN || !CONFIG.TELEGRAM_CHAT_ID) return;
  const agent = CONFIG.TELEGRAM_PROXY ? new SocksProxyAgent(CONFIG.TELEGRAM_PROXY) : undefined;
  axios.post(
    `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/sendMessage`,
    { chat_id: CONFIG.TELEGRAM_CHAT_ID, text },
    agent ? { httpsAgent: agent } : {}
  ).then(() => log('INFO', 'Telegram sent'))
   .catch(e => log('ERROR', `Telegram: ${e.message}`));
}

// ─── REST signing ─────────────────────────────────────────────────────────────
function signRest(method, endpoint, query, body) {
  const ts       = Math.floor(Date.now() / 1000).toString();
  const fullPath = '/api/v4' + endpoint;
  const bodyHash = crypto.createHash('sha512').update(body || '').digest('hex');
  const str      = `${method}\n${fullPath}\n${query || ''}\n${bodyHash}\n${ts}`;
  const sig      = crypto.createHmac('sha512', CONFIG.GATE_API_SECRET).update(str).digest('hex');
  return { ts, sig };
}

async function gateRest(method, endpoint, query, data) {
  const url  = CONFIG.GATE_BASE + endpoint;
  const qs   = query ? new URLSearchParams(query).toString() : '';
  const body = data  ? JSON.stringify(data) : '';
  const { ts, sig } = signRest(method, endpoint, qs, body);
  const res  = await axios({
    method,
    url: qs ? `${url}?${qs}` : url,
    headers: { 'KEY': CONFIG.GATE_API_KEY, 'SIGN': sig, 'Timestamp': ts, 'Content-Type': 'application/json' },
    data: data || undefined,
    timeout: 8000,
  });
  return res.data;
}

// ─── Contracts cache ──────────────────────────────────────────────────────────
const contractsCache       = new Map();
let contractsCacheUpdatedAt = 0;
const CONTRACTS_TTL         = 5 * 60 * 1000;

async function updateContractsCache() {
  try {
    const res = await axios.get('https://api.gateio.ws/api/v4/futures/usdt/contracts', { timeout: 10000 });
    contractsCache.clear();
    for (const c of res.data) {
      contractsCache.set(c.name, {
        markPrice: parseFloat(c.mark_price),
        quanto:    parseFloat(c.quanto_multiplier || '1'),
      });
    }
    contractsCacheUpdatedAt = Date.now();
    log('INFO', `Contracts cache: ${contractsCache.size} contracts`);
  } catch(e) {
    log('ERROR', `Contracts cache failed: ${e.message}`);
  }
}

function getContractInfo(ticker) {
  const contract = `${ticker}_USDT`;
  if (!contractsCache.has(contract)) return null;
  return { contract, ...contractsCache.get(contract) };
}

// ─── Balance cache ────────────────────────────────────────────────────────────
let cachedBalance     = null;
let balanceUpdatedAt  = 0;
const BALANCE_TTL_MS  = 5000;

async function getFreshBalance() {
  const acc     = await gateRest('GET', '/futures/usdt/accounts', null, null);
  cachedBalance = parseFloat(acc.available);
  balanceUpdatedAt = Date.now();
  return cachedBalance;
}

setInterval(async () => {
  try { await getFreshBalance(); } catch(e) {}
}, BALANCE_TTL_MS);

// ─── Gate.io WebSocket для ордерів ────────────────────────────────────────────
let gateWs          = null;
let gateWsReady     = false;
let gateWsReqId     = 1;
const gateWsPending = new Map(); // reqId → { resolve, reject, timer }

function signWs(ts) {
  return crypto.createHmac('sha512', CONFIG.GATE_API_SECRET)
    .update(`api\n${ts}`)
    .digest('hex');
}

function connectGateWs() {
  log('INFO', '[GateWS] Connecting...');
  gateWs      = new WebSocket(CONFIG.GATE_WS_URL);
  gateWsReady = false;

  gateWs.on('open', () => {
    log('INFO', '[GateWS] Connected — authenticating...');
    const ts  = Math.floor(Date.now() / 1000);
    const sig = signWs(ts);
    gateWs.send(JSON.stringify({
      time:       ts,
      channel:    'futures.login',
      event:      'api',
      request_id: 'auth-' + ts,
      payload: {
        api_key:   CONFIG.GATE_API_KEY,
        signature: sig,
        timestamp: String(ts),
      },
    }));
  });

  gateWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      // Auth response
      if (msg.channel === 'futures.login') {
        if (msg.event === 'api' && msg.result?.status === 'success') {
          gateWsReady = true;
          log('INFO', '[GateWS] Authenticated! Ready for WS orders.');
        } else {
          log('ERROR', `[GateWS] Auth failed: ${JSON.stringify(msg)}`);
        }
        return;
      }

      // Order response
      if (msg.channel === 'futures.order_place') {
        const reqId = msg.request_id || msg.id;
        const pend  = gateWsPending.get(reqId);
        if (!pend) return;
        clearTimeout(pend.timer);
        gateWsPending.delete(reqId);

        if (msg.errs || (msg.result && msg.result.errs)) {
          pend.reject(new Error(JSON.stringify(msg.errs || msg.result?.errs)));
        } else {
          pend.resolve(msg.result || msg);
        }
      }
    } catch(e) {
      log('ERROR', `[GateWS] Parse: ${e.message}`);
    }
  });

  gateWs.on('error', (e) => {
    log('ERROR', `[GateWS] Error: ${e.message}`);
    gateWsReady = false;
  });

  gateWs.on('close', (code) => {
    log('WARN', `[GateWS] Closed: ${code} — reconnecting in 2s...`);
    gateWsReady = false;
    // Відхиляємо всі pending запити
    for (const [id, pend] of gateWsPending.entries()) {
      clearTimeout(pend.timer);
      pend.reject(new Error('WS disconnected'));
      gateWsPending.delete(id);
    }
    setTimeout(connectGateWs, 2000);
  });

  // Ping кожні 20 сек щоб з'єднання не закривалось
  const pingInterval = setInterval(() => {
    if (gateWs.readyState === WebSocket.OPEN) {
      gateWs.send(JSON.stringify({ time: Math.floor(Date.now()/1000), channel: 'futures.ping' }));
    } else {
      clearInterval(pingInterval);
    }
  }, 20000);
}

// Відправляємо ордер через WebSocket
function placeOrderWs(contract, size) {
  return new Promise((resolve, reject) => {
    if (!gateWsReady || !gateWs || gateWs.readyState !== WebSocket.OPEN) {
      return reject(new Error('GateWS not ready'));
    }

    const reqId = String(gateWsReqId++);
    const ts    = Math.floor(Date.now() / 1000);
    const payload = {
      contract,
      size,
      price:  '0',
      tif:    'ioc',
      text:   't-listing-ws',
    };

    const timer = setTimeout(() => {
      gateWsPending.delete(reqId);
      reject(new Error('WS order timeout'));
    }, CONFIG.ORDER_TIMEOUT_MS);

    gateWsPending.set(reqId, { resolve, reject, timer });

    gateWs.send(JSON.stringify({
      time:       ts,
      channel:    'futures.order_place',
      event:      'api',
      request_id: reqId,
      payload,
    }));
  });
}

// ─── Open order: WS першочергово, REST як fallback ────────────────────────────
async function openOrder(contract, size) {
  // Спробуємо через WS
  if (gateWsReady) {
    try {
      const result = await placeOrderWs(contract, size);
      log('INFO', `[WS Order] Opened via WebSocket`);
      return result;
    } catch(e) {
      log('WARN', `[WS Order] Failed, falling back to REST: ${e.message}`);
    }
  }

  // Fallback: REST з retry
  for (let attempt = 1; attempt <= 30; attempt++) {
    try {
      const order = await gateRest('POST', '/futures/usdt/orders', null, {
        contract, size, price: '0', tif: 'ioc', text: 't-listing-rest',
      });
      log('INFO', `[REST Order] Opened on attempt ${attempt}`);
      return order;
    } catch(e) {
      const msg = e.response?.data?.message || e.message;
      log('WARN', `[REST] Attempt ${attempt}/30: ${msg}`);
      if (attempt < 30) await new Promise(r => setTimeout(r, 1000));
      else throw e;
    }
  }
}

// ─── Handle listing ───────────────────────────────────────────────────────────
async function handleNewListing(ticker, seenAt) {
  if (processedTickers.has(ticker)) return;
  processedTickers.set(ticker, Date.now());

  log('INFO', `NEW LISTING [Upbit WS]: ${ticker}`);

  // Паралельно: контракт з кешу + свіжий баланс
  let contractInfo = getContractInfo(ticker);

  // Якщо немає в кеші — пробуємо REST
  if (!contractInfo) {
    try {
      const res      = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts/${ticker}_USDT`, { timeout: 3000 });
      contractInfo   = {
        contract:  `${ticker}_USDT`,
        markPrice: parseFloat(res.data.mark_price),
        quanto:    parseFloat(res.data.quanto_multiplier || '1'),
      };
      // Додаємо в кеш
      contractsCache.set(`${ticker}_USDT`, { markPrice: contractInfo.markPrice, quanto: contractInfo.quanto });
    } catch(e) {
      log('WARN', `No contract for ${ticker}: ${e.message}`);
      sendTelegram(`[Upbit WS] Немає контракту для ${ticker} на Gate.io`);
      return;
    }
  } else {
    // Оновлюємо актуальну ціну з REST (швидко, контракт відомий)
    try {
      const res = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts/${ticker}_USDT`, { timeout: 3000 });
      contractInfo.markPrice = parseFloat(res.data.mark_price);
      contractInfo.quanto    = parseFloat(res.data.quanto_multiplier || '1');
    } catch(e) { /* використовуємо кешовану ціну */ }
  }

  const { contract, markPrice, quanto } = contractInfo;

  // Свіжий баланс
  const available = await getFreshBalance();

  // Встановлення плеча (не чекаємо)
  gateRest('POST', `/futures/usdt/positions/${contract}/leverage`,
    { leverage: '0', cross_leverage_limit: String(CONFIG.LEVERAGE) }, null
  ).catch(e => log('WARN', `Leverage: ${e.response?.data?.message || e.message}`));

  // Розрахунок розміру (65% балансу з урахуванням буфера Gate.io)
  const targetMargin = available * 0.65;
  const size         = Math.max(1, Math.floor((targetMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  const posValue     = (size * markPrice * quanto).toFixed(2);

  log('INFO', `Opening [WS]: ${contract} size=${size} price=${markPrice} quanto=${quanto} value=$${posValue} available=${available}`);

  let order;
  try {
    order = await openOrder(contract, size);
  } catch(e) {
    log('ERROR', `Order failed: ${e.response?.data?.message || e.message}`);
    sendTelegram(`ПОМИЛКА [Upbit WS] ${contract}:\n${e.response?.data?.message || e.message}`);
    // Скидаємо processedTickers щоб можна було спробувати знову
    processedTickers.delete(ticker);
    return;
  }

  const entryPrice = parseFloat(order.fill_price || order?.result?.fill_price) || markPrice;
  const elapsedSec = ((Date.now() - seenAt) / 1000).toFixed(2);
  const method     = order.text?.includes('ws') || !order.fill_price ? 'WS' : 'REST';

  cachedBalance = null; // скидаємо кеш балансу

  sendTelegram(
    `ПОЗИЦІЯ ВІДКРИТА 🟢\n` +
    `─────────────────────\n` +
    `📌 Монета:     ${ticker}\n` +
    `🔗 Джерело:    UPBIT WS\n` +
    `📄 Контракт:   ${contract}\n` +
    `💵 Ціна входу: ${entryPrice} USDT\n` +
    `📊 Розмір:     $${posValue}\n` +
    `⚡️ Плече:      ${CONFIG.LEVERAGE}x\n` +
    `⚙️ Метод:      ${method} ордер\n` +
    `⏱ Швидкість:   ${elapsedSec} сек\n` +
    `─────────────────────`
  );

  log('INFO', `Opened ${contract} entry=${entryPrice} method=${method} speed=${elapsedSec}s`);
}

// ─── Upbit анонси (резерв через проксі) ──────────────────────────────────────
const seenNoticeIds  = new Set();
let noticesInitialized = false;
let noticeRunning      = false;

const LISTING_KEYWORDS = ['추가', '신규 상장', '거래 지원', '디지털 자산 추가', '신규 거래지원', '거래지원 안내'];
const SKIP_KEYWORDS    = ['입출금', '점검', '이벤트', '중단', '종료', '폐지', '유의'];

function extractTickerFromTitle(title) {
  const matches = title.match(/\(([A-Z]{2,10})\)/g) || [];
  return matches.map(m => m.replace(/[()]/g, ''));
}

async function tickNotices() {
  if (noticeRunning) return;
  noticeRunning = true;
  try {
    const agent = CONFIG.TELEGRAM_PROXY ? new SocksProxyAgent(CONFIG.TELEGRAM_PROXY) : undefined;
    const res   = await axios.get(
      'https://api-manager.upbit.com/api/v1/announcements?os=moweb&page=1&per_page=20&category=all',
      {
        httpsAgent: agent,
        headers: {
          'User-Agent':      'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15',
          'Accept':          'application/json',
          'Accept-Language': 'ko-KR,ko;q=0.9',
          'Origin':          'https://upbit.com',
          'Referer':         'https://upbit.com/service-center/notice',
        },
        timeout: 10000,
      }
    );

    const notices = res.data?.data?.notices || [];
    const seenAt  = Date.now();

    if (!noticesInitialized) {
      notices.forEach(n => seenNoticeIds.add(n.id));
      log('INFO', `[Notices] Initialized with ${seenNoticeIds.size} notices`);
      noticesInitialized = true;
      return;
    }

    for (const notice of notices) {
      if (seenNoticeIds.has(notice.id)) continue;
      seenNoticeIds.add(notice.id);

      const title = notice.title || '';
      log('INFO', `[Notice] New: ${title}`);

      if (SKIP_KEYWORDS.some(w => title.includes(w))) continue;
      if (!LISTING_KEYWORDS.some(w => title.includes(w))) {
        sendTelegram(`🟡 Upbit анонс (перевір):\n${title}`);
        continue;
      }

      const tickers = extractTickerFromTitle(title);
      if (tickers.length === 0) {
        sendTelegram(`🟡 Upbit лістинг без тікера:\n${title}`);
        continue;
      }

      log('INFO', `[Notice] LISTING: ${tickers.join(', ')}`);
      for (const ticker of tickers) {
        await handleNewListing(ticker, seenAt);
      }
    }
  } catch(e) {
    log('ERROR', `[Notices] Tick: ${e.message}`);
  } finally {
    noticeRunning = false;
  }
}

// ─── CoinListing WebSocket (сигнали Upbit) ────────────────────────────────────
function startCoinListingWS() {
  const ws = new WebSocket(CONFIG.COINLISTING_URL);

  ws.on('open', () => log('INFO', '[CoinListing] Connected to seoul.coinlisting.pro'));

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'connection') {
        log('INFO', `[CoinListing] Auth OK | tier=${msg.tier} | delay=${msg.delay_ms}ms`);
        return;
      }
      log('INFO', `[CoinListing] MSG: ${JSON.stringify(msg)}`);
      if (msg.source === 'UPBIT' && msg.coins?.length > 0) {
        const seenAt = Date.now();
        for (const ticker of msg.coins) {
          if (ticker && ticker !== '***') {
            log('INFO', `[CoinListing] UPBIT LISTING: ${ticker}`);
            await handleNewListing(ticker, seenAt);
          }
        }
      }
    } catch(e) {
      log('ERROR', `[CoinListing] Parse: ${e.message}`);
    }
  });

  ws.on('error', e => log('ERROR', `[CoinListing] Error: ${e.message}`));

  ws.on('close', (code) => {
    log('WARN', `[CoinListing] Disconnected: ${code} — reconnecting in 3s...`);
    sendTelegram('⚠️ CoinListing WS відключився! Перепідключаємось...');
    setTimeout(startCoinListingWS, 3000);
  });
}

// ─── crix_master (резерв) ─────────────────────────────────────────────────────
async function tick() {
  if (isRunning) return;
  isRunning = true;
  try {
    const tsRes  = await axios.get('https://crix-static.upbit.com/v2/crix_master_timestamp', {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Origin': 'https://upbit.com' },
      timeout: 5000,
    });
    const checksum = tsRes.data?.checksum;
    if (lastChecksum && lastChecksum === checksum) { isRunning = false; return; }
    lastChecksum = checksum;

    const res   = await axios.get(CONFIG.UPBIT_CRIX_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json', 'Origin': 'https://upbit.com' },
      timeout: 30000,
    });
    const coins  = Array.isArray(res.data) ? res.data : [];
    const seenAt = Date.now();

    if (!initialized) {
      for (const coin of coins) {
        const code = coin.code || coin.baseCurrencyCode;
        if (code) knownCoins.set(code, coin);
      }
      log('INFO', `Initialized with ${knownCoins.size} coins`);
      initialized = true;
      saveState();
      isRunning = false;
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
  } catch(e) {
    log('ERROR', `Tick: ${e.message}`);
  } finally {
    isRunning = false;
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  log('INFO', '══════════════════════════════════════════');
  log('INFO', ' Upbit Listing Bot WS — starting up');
  log('INFO', '══════════════════════════════════════════');
  log('INFO', `LEV=${CONFIG.LEVERAGE}x | Gate.io WebSocket orders`);

  loadState();

  sendTelegram(
    'Upbit WS Bot запущен\n' +
    'Gate.io WebSocket ордери\n' +
    `Плече: ${CONFIG.LEVERAGE}x`
  );

  // 1. Кеш контрактів
  await updateContractsCache();
  setInterval(updateContractsCache, CONTRACTS_TTL);

  // 2. Баланс
  await getFreshBalance();

  // 3. Gate.io WebSocket для ордерів
  connectGateWs();

  // 4. CoinListing WebSocket (сигнали)
  startCoinListingWS();

  // 5. crix_master резерв
  await tick();
  setInterval(tick, CONFIG.POLL_INTERVAL_MS);

  // 6. Notices резерв
  await tickNotices();
  setInterval(tickNotices, 60000);

  // 7. Heartbeat
  setInterval(() => {
    const wsStatus = gateWsReady ? 'ready' : 'not ready';
    log('INFO', `Heartbeat | GateWS: ${wsStatus}`);
    sendTelegram(`✅ Upbit WS Bot живий\nGate.io WS: ${wsStatus}\n${new Date().toISOString()}`);
  }, 60 * 60 * 1000);
}

process.on('uncaughtException', (e) => {
  log('ERROR', `Uncaught: ${e.message}`);
  sendTelegram(`⚠️ UPBIT WS БОТ: помилка\n${e.message}`);
});

process.on('unhandledRejection', (e) => {
  log('ERROR', `Unhandled: ${e?.message || e}`);
  sendTelegram(`⚠️ UPBIT WS БОТ: помилка\n${e?.message || e}`);
});

main().catch(e => {
  log('ERROR', `Fatal: ${e.message}`);
  sendTelegram(`UPBIT WS БОТ ВПАВ!\n${e.message}`);
  setTimeout(() => process.exit(1), 1000);
});
