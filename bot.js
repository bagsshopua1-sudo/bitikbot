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
  BINANCE_SPOT_URL: 'https://api.binance.com/api/v3/exchangeInfo',
  BINANCE_FUTURES_URL: 'https://fapi.binance.com/fapi/v1/exchangeInfo',
  ORDER_RETRIES:    10,
  ORDER_RETRY_MS:   300,
};

// ─── State ────────────────────────────────────────────────────────────────────
const knownUpbitCoins    = new Map();
const knownBinanceSpot   = new Set();
const knownBinanceFutures = new Set();
let upbitInitialized    = false;
let binanceInitialized  = false;

const processedTickers = new Map();
const TICKER_TTL_MS = 24 * 60 * 60 * 1000;

setInterval(() => {
  const now = Date.now();
  for (const [t, ts] of processedTickers.entries()) {
    if (now - ts > TICKER_TTL_MS) processedTickers.delete(t);
  }
}, 60 * 60 * 1000);

// ─── Telegram ─────────────────────────────────────────────────────────────────
async function sendTelegram(text) {
  if (!CONFIG.TELEGRAM_TOKEN || !CONFIG.TELEGRAM_CHAT_ID) return;
  try {
    const agent = CONFIG.TELEGRAM_PROXY ? new SocksProxyAgent(CONFIG.TELEGRAM_PROXY) : undefined;
    await axios.post(
      `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/sendMessage`,
      { chat_id: CONFIG.TELEGRAM_CHAT_ID, text },
      agent ? { httpsAgent: agent } : {}
    );
    log('INFO', `Telegram sent`);
  } catch (e) {
    log('ERROR', `Telegram: ${e.message}`);
  }
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
  });
  return res.data;
}

async function contractExists(ticker) {
  const contract = `${ticker}_USDT`;
  try {
    const res = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts/${contract}`);
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

// ─── Retry ордер ──────────────────────────────────────────────────────────────
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

// ─── Відкриття позиції ────────────────────────────────────────────────────────
async function handleNewListing(ticker, source, seenAt) {
  if (processedTickers.has(ticker)) {
    log('INFO', `${ticker} already processed, skipping`);
    return;
  }
  processedTickers.set(ticker, Date.now());

  log('INFO', `NEW LISTING [${source}]: ${ticker}`);

  const { exists, contract, markPrice, quanto } = await contractExists(ticker);
  if (!exists) {
    log('WARN', `No Gate.io contract for ${ticker}`);
    await sendTelegram(`[${source}] Немає контракту для ${ticker} на Gate.io`);
    return;
  }

  const account = await gateRequest('GET', '/futures/usdt/accounts', null, null);
  const available = parseFloat(account.available);

  try {
    await gateRequest('POST', `/futures/usdt/positions/${contract}/leverage`,
      { leverage: '0', cross_leverage_limit: String(CONFIG.LEVERAGE) }, null);
  } catch(e) {
    log('WARN', `Leverage: ${e.response?.data?.message || e.message}`);
  }

  const useMargin = available * 0.9;
  const size = Math.max(1, Math.floor((useMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  const posValue = (size * markPrice * quanto).toFixed(2);

  let order;
  try {
    order = await openOrderWithRetry(contract, size);
  } catch (e) {
    log('ERROR', `All retries failed: ${e.response?.data?.message || e.message}`);
    await sendTelegram(`ПОМИЛКА [${source}] ${contract}: ${e.response?.data?.message || e.message}`);
    return;
  }

  const entryPrice = parseFloat(order.fill_price) || markPrice;
  const elapsedSec = ((Date.now() - seenAt) / 1000).toFixed(2);

  await sendTelegram(
    `ПОЗИЦІЯ ВІДКРИТА\n\n` +
    `Джерело: ${source}\n` +
    `Монета: ${ticker}\n` +
    `Контракт: ${contract}\n` +
    `Ціна входу: ${entryPrice} USDT\n` +
    `Розмір: $${posValue}\n` +
    `Плече: ${CONFIG.LEVERAGE}x\n` +
    `Швидкість: ${elapsedSec} сек`
  );

  log('INFO', `[${source}] Opened ${contract} entry=${entryPrice} value=$${posValue} speed=${elapsedSec}s`);
}

// ─── Upbit тік ────────────────────────────────────────────────────────────────
let upbitRunning = false;

async function tickUpbit() {
  if (upbitRunning) return;
  upbitRunning = true;
  try {
    const res = await axios.get(CONFIG.UPBIT_CRIX_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json', 'Origin': 'https://upbit.com' },
      timeout: 10000,
    });
    const coins = Array.isArray(res.data) ? res.data : [];
    const seenAt = Date.now();

    if (!upbitInitialized) {
      for (const coin of coins) {
        const code = coin.code || coin.baseCurrencyCode;
        if (code) knownUpbitCoins.set(code, coin);
      }
      log('INFO', `[Upbit] Initialized with ${knownUpbitCoins.size} coins`);
      upbitInitialized = true;
      return;
    }

    for (const coin of coins) {
      const code = coin.baseCurrencyCode || coin.code;
      if (!code) continue;
      if (coin.quoteCurrencyCode !== 'KRW') continue;
      if (coin.marketState !== 'ACTIVE') continue;
      if (!knownUpbitCoins.has(coin.code || code)) {
        knownUpbitCoins.set(coin.code || code, coin);
        await handleNewListing(code, 'UPBIT', seenAt);
      }
    }
  } catch (e) {
    log('ERROR', `[Upbit] Tick: ${e.message}`);
  } finally {
    upbitRunning = false;
  }
}

// ─── Binance тік ──────────────────────────────────────────────────────────────
let binanceRunning = false;

async function tickBinance() {
  if (binanceRunning) return;
  binanceRunning = true;
  try {
    const seenAt = Date.now();

    // Spot
    const spotRes = await axios.get(CONFIG.BINANCE_SPOT_URL, { timeout: 10000 });
    const spotSymbols = spotRes.data.symbols
      .filter(s => s.quoteAsset === 'USDT' && s.status === 'TRADING')
      .map(s => s.baseAsset);

    // Futures
    const futRes = await axios.get(CONFIG.BINANCE_FUTURES_URL, { timeout: 10000 });
    const futSymbols = futRes.data.symbols
      .filter(s => s.quoteAsset === 'USDT' && s.status === 'TRADING')
      .map(s => s.baseAsset);

    if (!binanceInitialized) {
      spotSymbols.forEach(s => knownBinanceSpot.add(s));
      futSymbols.forEach(s => knownBinanceFutures.add(s));
      log('INFO', `[Binance] Initialized: ${knownBinanceSpot.size} spot, ${knownBinanceFutures.size} futures`);
      binanceInitialized = true;
      return;
    }

    // Нові на споті
    for (const symbol of spotSymbols) {
      if (!knownBinanceSpot.has(symbol)) {
        knownBinanceSpot.add(symbol);
        log('INFO', `[Binance Spot] New: ${symbol}`);
        await handleNewListing(symbol, 'BINANCE_SPOT', seenAt);
      }
    }

    // Нові на ф'ючерсах
    for (const symbol of futSymbols) {
      if (!knownBinanceFutures.has(symbol)) {
        knownBinanceFutures.add(symbol);
        log('INFO', `[Binance Futures] New: ${symbol}`);
        await handleNewListing(symbol, 'BINANCE_FUTURES', seenAt);
      }
    }
  } catch (e) {
    log('ERROR', `[Binance] Tick: ${e.message}`);
  } finally {
    binanceRunning = false;
  }
}

// ─── Старт ────────────────────────────────────────────────────────────────────
async function main() {
  log('INFO', '══════════════════════════════════════');
  log('INFO', ' Upbit + Binance Listing Bot v3');
  log('INFO', '══════════════════════════════════════');
  log('INFO', `LEV=${CONFIG.LEVERAGE}x | Retry: ${CONFIG.ORDER_RETRIES}x/${CONFIG.ORDER_RETRY_MS}ms`);

  await sendTelegram(
    'Listing Bot v3 запущен\n' +
    'Джерела: Upbit + Binance Spot + Binance Futures\n' +
    `Плече: ${CONFIG.LEVERAGE}x | Retry: ${CONFIG.ORDER_RETRIES}x`
  );

  // Ініціалізація
  await tickUpbit();
  await tickBinance();

  // Опитування кожні 2 секунди
  setInterval(tickUpbit, CONFIG.POLL_INTERVAL_MS);
  setInterval(tickBinance, CONFIG.POLL_INTERVAL_MS);
}

// Повідомлення при падінні
process.on('uncaughtException', async (e) => {
  log('ERROR', `Uncaught: ${e.message}`);
  try { await sendTelegram(`UPBIT БОТ ВПАВ!\nПомилка: ${e.message}\nПерезапускається...`); } catch(_) {}
  process.exit(1);
});

process.on('unhandledRejection', async (e) => {
  log('ERROR', `Unhandled: ${e?.message || e}`);
  try { await sendTelegram(`UPBIT БОТ ВПАВ!\nПомилка: ${e?.message || e}\nПерезапускається...`); } catch(_) {}
  process.exit(1);
});

main().catch(async e => {
  log('ERROR', `Fatal: ${e.message}`);
  try { await sendTelegram(`UPBIT БОТ ВПАВ!\nПомилка: ${e.message}\nПерезапускається...`); } catch(_) {}
  process.exit(1);
});
