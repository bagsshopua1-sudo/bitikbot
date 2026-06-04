require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

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
  LEVERAGE:         parseInt(process.env.LEVERAGE || '10'),
  POLL_INTERVAL_MS: 2000,
  GATE_BASE:        'https://api.gateio.ws/api/v4',
  UPBIT_CRIX_URL:   'https://crix-static.upbit.com/v2/crix_master',
};

// Хранит все известные монеты { code -> данные }
const knownCoins = new Map();
let initialized = false;

// Защита от дублей — тикеры обработанные за 24ч
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
    await axios.post(
      `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/sendMessage`,
      { chat_id: CONFIG.TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }
    );
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

// ─── Открытие позиции ─────────────────────────────────────────────────────────
async function handleNewListing(ticker, coinData, seenAt) {
  if (processedTickers.has(ticker)) {
    log('INFO', `Ticker ${ticker} already processed, skipping`);
    return;
  }
  processedTickers.set(ticker, Date.now());

  log('INFO', `🚀 NEW LISTING detected: ${ticker}`);
  log('INFO', `Coin data: ${JSON.stringify(coinData)}`);

  // Проверяем контракт на Gate.io
  const { exists, contract, markPrice, quanto } = await contractExists(ticker);
  if (!exists) {
    log('WARN', `No Gate.io contract for ${ticker}`);
    await sendTelegram(
      `🟠 <b>НОВЫЙ ЛИСТИНГ НА UPBIT</b>\n` +
      `Монета: <b>${ticker}</b>\n` +
      `❌ Контракт не найден на Gate.io Futures`
    );
    return;
  }

  // Баланс
  const account = await gateRequest('GET', '/futures/usdt/accounts', null, null);
  const available = parseFloat(account.available);

  // Кросс-плечо
  try {
    await gateRequest('POST', `/futures/usdt/positions/${contract}/leverage`,
      { leverage: '0', cross_leverage_limit: String(CONFIG.LEVERAGE) }, null);
  } catch(e) {
    log('WARN', `Leverage: ${e.response?.data?.message || e.message}`);
  }

  // Размер: 90% баланса × плечо / цена
  const useMargin = available * 0.9;
  const size = Math.max(1, Math.floor((useMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  const posValue = (size * markPrice * quanto).toFixed(2);

  log('INFO', `Opening: ${contract} size=${size} price=${markPrice} value=$${posValue}`);

  let order;
  try {
    order = await gateRequest('POST', '/futures/usdt/orders', null, {
      contract, size, price: '0', tif: 'ioc', text: 't-listing',
    });
  } catch (e) {
    log('ERROR', `Order failed: ${e.response?.data?.message || e.message}`);
    await sendTelegram(`❌ <b>ОШИБКА ОРДЕРА</b>\n${contract}\n${e.response?.data?.message || e.message}`);
    return;
  }

  const entryPrice = parseFloat(order.fill_price) || markPrice;
  const elapsedSec = ((Date.now() - seenAt) / 1000).toFixed(2);
  const now = new Date().toISOString();

  await sendTelegram(
    `🚀 <b>ПОЗИЦИЯ ОТКРЫТА</b>\n\n` +
    `📌 Монета: <b>${ticker}</b>\n` +
    `📄 Контракт: <code>${contract}</code>\n` +
    `💵 Цена входа: <b>${entryPrice} USDT</b>\n` +
    `📊 Размер позиции: <b>$${posValue}</b>\n` +
    `⚡️ Плечо: <b>${CONFIG.LEVERAGE}x (кросс)</b>\n` +
    `⏱ Скорость: <b>${elapsedSec} сек</b>\n` +
    `🕐 Время: ${now}`
  );

  log('INFO', `Position opened: ${contract} entry=${entryPrice} value=$${posValue} speed=${elapsedSec}s`);
}

// ─── Получить список монет с Upbit ────────────────────────────────────────────
async function fetchUpbitCoins() {
  const res = await axios.get(CONFIG.UPBIT_CRIX_URL, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Accept': 'application/json',
      'Origin': 'https://upbit.com',
      'Referer': 'https://upbit.com/',
    },
    timeout: 10000,
  });
  return Array.isArray(res.data) ? res.data : [];
}

// ─── Главный тик ──────────────────────────────────────────────────────────────
let isRunning = false;

async function tick() {
  if (isRunning) return;
  isRunning = true;

  try {
    const coins = await fetchUpbitCoins();
    const seenAt = Date.now();

    if (!initialized) {
      // Первый запуск — запоминаем все монеты как известные
      for (const coin of coins) {
        const code = coin.code || coin.baseCurrencyCode;
        if (code) knownCoins.set(code, coin);
      }
      log('INFO', `Initialized with ${knownCoins.size} coins from Upbit`);
      initialized = true;
      return;
    }

    // Ищем новые монеты
    for (const coin of coins) {
      const code = coin.baseCurrencyCode || coin.code;
      if (!code) continue;

      // Только KRW пары и активные
      if (coin.quoteCurrencyCode !== 'KRW') continue;
      if (coin.marketState !== 'ACTIVE') continue;

      if (!knownCoins.has(coin.code || code)) {
        log('INFO', `NEW COIN FOUND: ${code} (${coin.koreanName || coin.englishName})`);
        knownCoins.set(coin.code || code, coin);
        await handleNewListing(code, coin, seenAt);
      }
    }

  } catch (e) {
    log('ERROR', `Tick: ${e.message}`);
  } finally {
    isRunning = false;
  }
}

// ─── Старт ────────────────────────────────────────────────────────────────────
async function main() {
  log('INFO', '══════════════════════════════════════');
  log('INFO', ' Upbit Listing Bot v2 — starting up');
  log('INFO', '══════════════════════════════════════');
  log('INFO', `LEV=${CONFIG.LEVERAGE}x | Метод: crix_master API`);

  await sendTelegram(
    '🤖 <b>Upbit Listing Bot v2 запущен</b>\n' +
    'Метод: прямой API Upbit (crix_master)\n' +
    'Мониторинг каждые 2 секунды'
  );

  await tick(); // инициализация
  setInterval(tick, CONFIG.POLL_INTERVAL_MS);
}

main().catch(e => {
  log('ERROR', `Fatal: ${e.message}`);
  process.exit(1);
});
