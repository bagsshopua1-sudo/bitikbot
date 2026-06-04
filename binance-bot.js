require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { SocksProxyAgent } = require('socks-proxy-agent');

const LOG_FILE = path.join(__dirname, 'binance-bot.log');

function log(level, message) {
  const ts = new Date().toISOString();
  const line = `[${ts}] [${level}] ${message}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

const CONFIG = {
  GATE_API_KEY:         process.env.GATE_API_KEY     || '',
  GATE_API_SECRET:      process.env.GATE_API_SECRET  || '',
  TELEGRAM_TOKEN:       process.env.TELEGRAM_TOKEN   || '',
  TELEGRAM_CHAT_ID:     process.env.TELEGRAM_CHAT_ID || '',
  TELEGRAM_PROXY:       process.env.TELEGRAM_PROXY   || '',
  LEVERAGE:             parseInt(process.env.LEVERAGE || '10'),
  POLL_INTERVAL_MS:     2000,
  GATE_BASE:            'https://api.gateio.ws/api/v4',
  BINANCE_ANNOUNCE_URL: 'https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=10',
  ORDER_RETRIES:        10,
  ORDER_RETRY_MS:       300,
};

const SKIP_WORDS    = ['delist', 'suspend', 'deprecat', 'remove', 'tradfi', 'pre-ipo', 'multiple'];
const LISTING_WORDS = ['will launch', 'perpetual', 'listing'];

const seenArticleIds   = new Set();
const processedTickers = new Map();
const TICKER_TTL_MS    = 24 * 60 * 60 * 1000;
let initialized = false;

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

// ─── Витягуємо тікери ─────────────────────────────────────────────────────────
function extractTickers(title) {
  const matches = [];
  const usdtMatches = title.match(/\b([A-Z]{2,10})USDT\b/g) || [];
  usdtMatches.forEach(m => matches.push(m.replace('USDT', '')));
  const bracketMatches = title.match(/\(([A-Z]{2,10})\)/g) || [];
  bracketMatches.forEach(m => matches.push(m.replace(/[()]/g, '')));
  const skipTokens = ['USDT', 'USD', 'BTC', 'ETH', 'BNB', 'BUSD', 'USDM', 'AED'];
  return [...new Set(matches)].filter(t => !skipTokens.includes(t));
}

// ─── Відкриття позиції з урахуванням розподілу балансу ───────────────────────
async function openPosition(ticker, marginPercent, seenAt, title) {
  if (processedTickers.has(ticker)) {
    log('INFO', `${ticker} already processed`);
    return false;
  }
  processedTickers.set(ticker, Date.now());

  log('INFO', `Trying to open: ${ticker} (margin: ${marginPercent}%)`);

  // Чекаємо до 30 сек поки контракт з'явиться на Gate.io
  let contractData = null;
  for (let i = 0; i < 10; i++) {
    contractData = await contractExists(ticker);
    if (contractData.exists) break;
    log('INFO', `Waiting for ${ticker}_USDT on Gate.io... (${i+1}/10)`);
    await new Promise(r => setTimeout(r, 3000));
  }

  if (!contractData.exists) {
    log('WARN', `No Gate.io contract for ${ticker}`);
    return false;
  }

  const { contract, markPrice, quanto } = contractData;
  const account = await gateRequest('GET', '/futures/usdt/accounts', null, null);
  const available = parseFloat(account.available);

  try {
    await gateRequest('POST', `/futures/usdt/positions/${contract}/leverage`,
      { leverage: '0', cross_leverage_limit: String(CONFIG.LEVERAGE) }, null);
  } catch(e) {
    log('WARN', `Leverage: ${e.response?.data?.message || e.message}`);
  }

  const useMargin = available * (marginPercent / 100);
  const size = Math.max(1, Math.floor((useMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  const posValue = (size * markPrice * quanto).toFixed(2);

  let order;
  try {
    order = await openOrderWithRetry(contract, size);
  } catch (e) {
    log('ERROR', `All retries failed for ${ticker}: ${e.response?.data?.message || e.message}`);
    await sendTelegram(`ПОМИЛКА ${contract}: ${e.response?.data?.message || e.message}`);
    return false;
  }

  const entryPrice = parseFloat(order.fill_price) || markPrice;
  const elapsedSec = ((Date.now() - seenAt) / 1000).toFixed(2);

  await sendTelegram(
    `ПОЗИЦІЯ ВІДКРИТА\n\n` +
    `Джерело: BINANCE ANNOUNCE\n` +
    `Монета: ${ticker}\n` +
    `Контракт: ${contract}\n` +
    `Ціна входу: ${entryPrice} USDT\n` +
    `Розмір: $${posValue} (${marginPercent}% балансу)\n` +
    `Плече: ${CONFIG.LEVERAGE}x\n` +
    `Швидкість: ${elapsedSec} сек`
  );

  log('INFO', `Opened ${contract} entry=${entryPrice} value=$${posValue} speed=${elapsedSec}s`);
  return true;
}

// ─── Обробка лістингу з розподілом балансу ───────────────────────────────────
async function handleListing(tickers, title, seenAt) {
  log('INFO', `Processing ${tickers.length} tickers: ${tickers.join(', ')}`);

  // Визначаємо який відсоток балансу на кожен тікер
  // 1 тікер → 90%, 2 тікери → 40% кожен, 3+ → рівномірно
  let marginPerTicker;
  if (tickers.length === 1) {
    marginPerTicker = 90;
  } else if (tickers.length === 2) {
    marginPerTicker = 40;
  } else {
    marginPerTicker = Math.floor(80 / tickers.length);
  }

  // Спочатку перевіряємо які тікери є на Gate.io
  const available = [];
  const notAvailable = [];

  for (const ticker of tickers) {
    const { exists } = await contractExists(ticker);
    if (exists) {
      available.push(ticker);
    } else {
      notAvailable.push(ticker);
    }
  }

  log('INFO', `Available on Gate.io: ${available.join(', ') || 'none'}`);
  log('INFO', `Not available: ${notAvailable.join(', ') || 'none'}`);

  // Якщо є недоступні — повідомляємо
  if (notAvailable.length > 0) {
    await sendTelegram(
      `Binance анонс: ${title}\n\n` +
      `Немає на Gate.io: ${notAvailable.join(', ')}\n` +
      `Торгуємо тільки: ${available.join(', ') || 'нічого'}`
    );
  }

  // Якщо доступний тільки 1 з 2 — використовуємо більший відсоток
  let finalMargin = marginPerTicker;
  if (tickers.length === 2 && available.length === 1) {
    finalMargin = 90; // весь баланс на єдиний доступний
    log('INFO', `Only 1 of 2 available — using 90% margin`);
  }

  // Відкриваємо позиції
  for (const ticker of available) {
    await openPosition(ticker, finalMargin, seenAt, title);
  }

  // Чекаємо недоступні — може з'являться пізніше
  if (notAvailable.length > 0) {
    log('INFO', `Waiting for unavailable tickers: ${notAvailable.join(', ')}`);
    for (const ticker of notAvailable) {
      // Чекаємо до 30 сек
      for (let i = 0; i < 10; i++) {
        const { exists } = await contractExists(ticker);
        if (exists) {
          log('INFO', `${ticker} appeared on Gate.io!`);
          await openPosition(ticker, available.length > 0 ? marginPerTicker : 90, seenAt, title);
          break;
        }
        await new Promise(r => setTimeout(r, 3000));
      }
    }
  }
}

// ─── Парсинг анонсів ──────────────────────────────────────────────────────────
let isRunning = false;

async function tick() {
  if (isRunning) return;
  isRunning = true;
  try {
    const res = await axios.get(CONFIG.BINANCE_ANNOUNCE_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0' },
      timeout: 10000,
    });

    const articles = res.data?.data?.articles || [];
    const seenAt = Date.now();

    if (!initialized) {
      articles.forEach(a => seenArticleIds.add(a.id));
      log('INFO', `Initialized with ${seenArticleIds.size} Binance articles`);
      initialized = true;
      return;
    }

    for (const article of articles) {
      if (seenArticleIds.has(article.id)) continue;
      seenArticleIds.add(article.id);

      const title = article.title || '';
      const titleLower = title.toLowerCase();

      log('INFO', `New article: ${title}`);

      if (SKIP_WORDS.some(w => titleLower.includes(w))) {
        log('INFO', `Skip: ${title}`);
        continue;
      }

      if (!LISTING_WORDS.some(w => titleLower.includes(w))) {
        await sendTelegram(`Binance анонс (перевір вручну):\n${title}`);
        continue;
      }

      const tickers = extractTickers(title);
      if (tickers.length === 0) {
        log('WARN', `No tickers in: ${title}`);
        await sendTelegram(`Binance лістинг без тікера:\n${title}`);
        continue;
      }

      log('INFO', `Tickers found: ${tickers.join(', ')}`);
      await handleListing(tickers, title, seenAt);
    }
  } catch (e) {
    log('ERROR', `Tick: ${e.message}`);
  } finally {
    isRunning = false;
  }
}

async function main() {
  log('INFO', '══════════════════════════════════════');
  log('INFO', ' Binance Announce Bot — starting up');
  log('INFO', '══════════════════════════════════════');
  log('INFO', `LEV=${CONFIG.LEVERAGE}x | 1 ticker=90% | 2 tickers=40% each`);

  await sendTelegram(
    'Binance Announce Bot запущен\n' +
    'Парсинг анонсів кожні 2 сек\n' +
    `Плече: ${CONFIG.LEVERAGE}x\n` +
    '1 тікер = 90% балансу\n' +
    '2 тікери = 40% кожен\n' +
    'Якщо 1 з 2 недоступний = 90% на доступний'
  );

  await tick();
  setInterval(tick, CONFIG.POLL_INTERVAL_MS);
}

main().catch(e => {
  log('ERROR', `Fatal: ${e.message}`);
  process.exit(1);
});
