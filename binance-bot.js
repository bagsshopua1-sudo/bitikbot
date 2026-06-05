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

// ─── Кеш балансу (оптимізація 1) ─────────────────────────────────────────────
let cachedBalance = null;
let balanceUpdatedAt = 0;
const BALANCE_TTL_MS = 5000;

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

// ─── Кеш контрактів Gate.io ───────────────────────────────────────────────────
const contractsCache = new Map();
let contractsCacheUpdatedAt = 0;
const CONTRACTS_TTL = 5 * 60 * 1000; // 5 хвилин

async function updateContractsCache() {
  try {
    const res = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts`, {
      params: { limit: 1000 },
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
    log('INFO', `Contracts cache updated: ${contractsCache.size} contracts`);
  } catch(e) {
    log('ERROR', `Contracts cache update failed: ${e.message}`);
  }
}

async function contractExists(ticker) {
  const contract = `${ticker}_USDT`;

  // Оновлюємо кеш якщо застарів
  if (Date.now() - contractsCacheUpdatedAt > CONTRACTS_TTL) {
    await updateContractsCache();
  }

  // Перевіряємо кеш (~0ms)
  if (contractsCache.has(contract)) {
    const data = contractsCache.get(contract);
    // Отримуємо актуальну ціну
    try {
      const res = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts/${contract}`, { timeout: 3000 });
      return {
        exists: true, contract,
        markPrice: parseFloat(res.data.mark_price),
        quanto: parseFloat(res.data.quanto_multiplier || '1'),
      };
    } catch(e) {
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

// ─── Відкриття позиції ────────────────────────────────────────────────────────
async function openPosition(ticker, marginPercent, seenAt) {
  if (processedTickers.has(ticker)) return false;
  processedTickers.set(ticker, Date.now());

  log('INFO', `Opening [Binance]: ${ticker} (${marginPercent}% margin)`);

  // Оптимізація 4: паралельно перевіряємо контракт і баланс
  const [contractData, available] = await Promise.all([
    contractExists(ticker),
    getCachedBalance(),
  ]);

  if (!contractData.exists) {
    log('WARN', `No Gate.io contract for ${ticker}`);
    return false;
  }

  const { contract, markPrice, quanto } = contractData;

  // Плече паралельно (не чекаємо)
  gateRequest('POST', `/futures/usdt/positions/${contract}/leverage`,
    { leverage: '0', cross_leverage_limit: String(CONFIG.LEVERAGE) }, null
  ).catch(e => log('WARN', `Leverage: ${e.response?.data?.message || e.message}`));

  const useMargin = available * (marginPercent / 100);
  const size = Math.max(1, Math.floor((useMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  const posValue = (size * markPrice * quanto).toFixed(2);

  let order;
  try {
    order = await openOrderWithRetry(contract, size);
  } catch (e) {
    log('ERROR', `All retries failed for ${ticker}: ${e.response?.data?.message || e.message}`);
    sendTelegram(`ПОМИЛКА [Binance] ${contract}: ${e.response?.data?.message || e.message}`);
    return false;
  }

  const entryPrice = parseFloat(order.fill_price) || markPrice;
  const elapsedSec = ((Date.now() - seenAt) / 1000).toFixed(2);

  // Скидаємо кеш балансу
  cachedBalance = null;

  sendTelegram(
    `ПОЗИЦІЯ ВІДКРИТА 🟢\n` +
    `─────────────────────\n` +
    `📌 Монета:     ${ticker}\n` +
    `🔗 Джерело:    BINANCE\n` +
    `📄 Контракт:   ${contract}\n` +
    `💵 Ціна входу: ${entryPrice} USDT\n` +
    `📊 Розмір:     $${posValue} (${marginPercent}%)\n` +
    `⚡️ Плече:      ${CONFIG.LEVERAGE}x\n` +
    `⏱ Швидкість:   ${elapsedSec} сек\n` +
    `─────────────────────`
  );

  log('INFO', `Opened ${contract} entry=${entryPrice} speed=${elapsedSec}s`);
  return true;
}

// ─── Розподіл балансу між тікерами ───────────────────────────────────────────
async function handleListing(tickers, seenAt) {
  // Оптимізація 4: паралельно перевіряємо всі контракти
  const checks = await Promise.all(tickers.map(t => contractExists(t)));
  const available = tickers.filter((t, i) => checks[i].exists);
  const notAvailable = tickers.filter((t, i) => !checks[i].exists);

  if (notAvailable.length > 0) {
    log('WARN', `Not on Gate.io: ${notAvailable.join(', ')}`);
    sendTelegram(`Binance лістинг — немає на Gate.io: ${notAvailable.join(', ')}`);
  }

  // Розподіл балансу
  let marginPerTicker;
  if (available.length === 0) return;
  if (tickers.length === 1) marginPerTicker = 90;
  else if (tickers.length === 2 && available.length === 1) marginPerTicker = 90;
  else if (tickers.length === 2) marginPerTicker = 40;
  else marginPerTicker = Math.floor(80 / available.length);

  // Оптимізація 4: відкриваємо всі позиції ПАРАЛЕЛЬНО
  await Promise.all(available.map(ticker => openPosition(ticker, marginPerTicker, seenAt)));

  // Чекаємо недоступні
  if (notAvailable.length > 0) {
    for (const ticker of notAvailable) {
      for (let i = 0; i < 10; i++) {
        const { exists } = await contractExists(ticker);
        if (exists) {
          await openPosition(ticker, available.length > 0 ? 40 : 90, seenAt);
          break;
        }
        await new Promise(r => setTimeout(r, 3000));
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

// ─── Парсинг анонсів ──────────────────────────────────────────────────────────
let isRunning = false;

async function tick() {
  if (isRunning) return;
  isRunning = true;
  try {
    const agent = CONFIG.TELEGRAM_PROXY ? new SocksProxyAgent(CONFIG.TELEGRAM_PROXY) : undefined;
    const res = await axios.get(CONFIG.BINANCE_ANNOUNCE_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0' },
      httpsAgent: agent,
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
        sendTelegram(`Binance анонс (перевір вручну):\n${title}`);
        continue;
      }

      const tickers = extractTickers(title);
      if (tickers.length === 0) {
        sendTelegram(`Binance лістинг без тікера:\n${title}`);
        continue;
      }

      log('INFO', `Tickers: ${tickers.join(', ')}`);
      await handleListing(tickers, seenAt);
    }
  } catch (e) {
    log('ERROR', `Tick: ${e.message}`);
  } finally {
    isRunning = false;
  }
}

// ─── CoinListing WebSocket (Binance Spot + Futures) ──────────────────────────
function startCoinListingWS() {
  const ws = new (require('ws'))('wss://tokyo.coinlisting.pro/listings?key=ilyak-2c3dbb');

  ws.on('open', () => {
    log('INFO', '[CoinListing] Connected to tokyo.coinlisting.pro');
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'connection') {
        log('INFO', `[CoinListing] Auth OK | tier=${msg.tier} | sources=${msg.sources?.join(',')}`);
        return;
      }

      log('INFO', `[CoinListing] MSG: ${JSON.stringify(msg)}`);

      // Фільтруємо тільки Binance лістинги (не делістинги)
      if (!msg.source || !msg.source.includes('BINANCE')) return;
      if (!msg.coins || msg.coins.length === 0) return;

      const title = msg.title || '';
      const titleLower = title.toLowerCase();

      // Пропускаємо делістинги і технічні
      if (SKIP_WORDS.some(w => titleLower.includes(w))) {
        log('INFO', `[CoinListing] Skip: ${title}`);
        return;
      }

      const seenAt = Date.now();
      const validTickers = msg.coins.filter(t => t && t !== '***');

      if (validTickers.length === 0) return;

      log('INFO', `[CoinListing] BINANCE LISTING: ${validTickers.join(', ')} | ${title}`);
      await handleListing(validTickers, seenAt);

    } catch(e) {
      log('ERROR', `[CoinListing] Parse error: ${e.message}`);
    }
  });

  ws.on('error', (e) => {
    log('ERROR', `[CoinListing] Error: ${e.message}`);
  });

  ws.on('close', (code) => {
    log('WARN', `[CoinListing] Disconnected: ${code} — reconnecting in 3s...`);
    setTimeout(startCoinListingWS, 3000);
  });
}

async function main() {
  log('INFO', '══════════════════════════════════════');
  log('INFO', ' Binance Announce Bot v3 — starting up');
  log('INFO', '══════════════════════════════════════');
  log('INFO', `LEV=${CONFIG.LEVERAGE}x | WebSocket + HTTP fallback`);

  sendTelegram(
    'Binance Listing Bot v3 запущен\n' +
    'WebSocket: tokyo.coinlisting.pro\n' +
    `Плече: ${CONFIG.LEVERAGE}x`
  );

  // Завантажуємо кеш контрактів
  await updateContractsCache();
  setInterval(updateContractsCache, CONTRACTS_TTL);

  // WebSocket — основний канал
  startCoinListingWS();

  // HTTP fallback — резерв кожні 30 сек
  await tick();
  setInterval(tick, 30000);
}

process.on('uncaughtException', (e) => {
  log('ERROR', `Uncaught: ${e.message}`);
  sendTelegram(`BINANCE БОТ ВПАВ!\n${e.message}\nПерезапускається...`);
  setTimeout(() => process.exit(1), 1000);
});

process.on('unhandledRejection', (e) => {
  log('ERROR', `Unhandled: ${e?.message || e}`);
  sendTelegram(`BINANCE БОТ ВПАВ!\n${e?.message || e}\nПерезапускається...`);
  setTimeout(() => process.exit(1), 1000);
});

main().catch(e => {
  log('ERROR', `Fatal: ${e.message}`);
  sendTelegram(`BINANCE БОТ ВПАВ!\n${e.message}`);
  setTimeout(() => process.exit(1), 1000);
});
