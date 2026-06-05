require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');
const { SocksProxyAgent } = require('socks-proxy-agent');

const CONFIG = {
  GATE_API_KEY:     process.env.GATE_API_KEY     || '',
  GATE_API_SECRET:  process.env.GATE_API_SECRET  || '',
  TELEGRAM_TOKEN:   process.env.TELEGRAM_TOKEN   || '',
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID || '',
  TELEGRAM_PROXY:   process.env.TELEGRAM_PROXY   || '',
  GATE_BASE:        'https://api.gateio.ws/api/v4',
};

function getAgent() {
  return CONFIG.TELEGRAM_PROXY ? new SocksProxyAgent(CONFIG.TELEGRAM_PROXY) : undefined;
}

async function sendTelegram(text, chatId) {
  const agent = getAgent();
  await axios.post(
    `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/sendMessage`,
    { chat_id: chatId || CONFIG.TELEGRAM_CHAT_ID, text },
    agent ? { httpsAgent: agent } : {}
  );
}

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
    timeout: 10000,
  });
  return res.data;
}

// ─── Команди ──────────────────────────────────────────────────────────────────
async function cmdBalance(chatId) {
  const acc = await gateRequest('GET', '/futures/usdt/accounts', null, null);
  const total     = parseFloat(acc.total).toFixed(2);
  const available = parseFloat(acc.available).toFixed(2);
  const unrealised = parseFloat(acc.unrealised_pnl || 0).toFixed(2);
  const pnlEmoji = parseFloat(unrealised) >= 0 ? '🟢' : '🔴';

  await sendTelegram(
    `💰 ─────────────────────\n` +
    `     БАЛАНС Gate.io\n` +
    `─────────────────────\n` +
    `📊 Всього:      $${total}\n` +
    `✅ Доступно:    $${available}\n` +
    `${pnlEmoji} Нереал. P&L: $${unrealised}\n` +
    `─────────────────────`,
    chatId
  );
}

async function cmdPnl(chatId) {
  const positions = await gateRequest('GET', '/futures/usdt/positions', null, null);
  const open = positions.filter(p => parseInt(p.size) !== 0);

  if (open.length === 0) {
    await sendTelegram(
      `📊 ─────────────────────\n` +
      `  Немає відкритих позицій\n` +
      `─────────────────────`,
      chatId
    );
    return;
  }

  for (const pos of open) {
    const pnl    = parseFloat(pos.unrealised_pnl || 0).toFixed(2);
    const pnlPct = (parseFloat(pos.unrealised_pnl_pcnt || 0) * 100).toFixed(2);
    const side   = parseInt(pos.size) > 0 ? '🟢 LONG' : '🔴 SHORT';
    const pnlEmoji = parseFloat(pnl) >= 0 ? '🟢' : '🔴';
    const ticker = pos.contract.replace('_USDT', '');

    await sendTelegram(
      `📈 ─────────────────────\n` +
      `   ${ticker} | ${side}\n` +
      `─────────────────────\n` +
      `📄 Контракт:  ${pos.contract}\n` +
      `💵 Вхід:      ${parseFloat(pos.entry_price).toFixed(6)}\n` +
      `📊 Поточна:   ${parseFloat(pos.mark_price).toFixed(6)}\n` +
      `${pnlEmoji} P&L:       $${pnl} (${pnlPct}%)\n` +
      `📦 Розмір:    ${Math.abs(parseInt(pos.size))} конт.\n` +
      `─────────────────────`,
      chatId
    );
  }
}

async function cmdTp(contract, price, chatId) {
  const pos = await gateRequest('GET', `/futures/usdt/positions/${contract}`, null, null);
  const size = parseInt(pos.size);
  if (size === 0) {
    await sendTelegram(`❌ Немає позиції по ${contract}`, chatId);
    return;
  }

  await gateRequest('POST', '/futures/usdt/orders', null, {
    contract,
    size: -size,
    price: price.toString(),
    tif: 'gtc',
    reduce_only: true,
    text: 't-tp-manual',
  });

  await sendTelegram(
    `🎯 ─────────────────────\n` +
    `   TAKE PROFIT виставлено\n` +
    `─────────────────────\n` +
    `📄 Контракт: ${contract}\n` +
    `💰 Ціна TP:  ${price} USDT\n` +
    `─────────────────────`,
    chatId
  );
}

async function cmdSl(contract, price, chatId) {
  const pos = await gateRequest('GET', `/futures/usdt/positions/${contract}`, null, null);
  const size = parseInt(pos.size);
  if (size === 0) {
    await sendTelegram(`❌ Немає позиції по ${contract}`, chatId);
    return;
  }

  const closeSize = size > 0 ? -Math.abs(size) : Math.abs(size);

  await gateRequest('POST', '/futures/usdt/price_orders', null, {
    initial: {
      contract,
      size: closeSize,
      price: '0',
      tif: 'ioc',
      text: 't-sl-manual',
    },
    trigger: {
      strategy_type: 0,
      price_type: 1,
      price: price.toString(),
      rule: size > 0 ? 2 : 1,
      expiration: 86400,
    },
  });

  await sendTelegram(
    `🛑 ─────────────────────\n` +
    `   STOP LOSS виставлено\n` +
    `─────────────────────\n` +
    `📄 Контракт: ${contract}\n` +
    `💸 Ціна SL:  ${price} USDT\n` +
    `─────────────────────`,
    chatId
  );
}

async function cmdClose(contract, chatId) {
  // Отримуємо всі відкриті ордери щоб знайти розмір
  const positions = await gateRequest('GET', '/futures/usdt/positions', null, null);
  const pos = positions.find(p => p.contract === contract && parseInt(p.size) !== 0);

  if (!pos) {
    await sendTelegram(`❌ Немає відкритої позиції по ${contract}`, chatId);
    return;
  }

  const size = parseInt(pos.size);
  const closeSize = size > 0 ? -Math.abs(size) : Math.abs(size);

  await gateRequest('POST', '/futures/usdt/orders', null, {
    contract,
    size: closeSize,
    price: '0',
    tif: 'ioc',
    text: 't-close-manual',
  });

  const entryPrice = parseFloat(pos.entry_price);
  const markPrice = parseFloat(pos.mark_price);
  const pnl = ((markPrice - entryPrice) * Math.abs(size)).toFixed(2);
  const pnlEmoji = parseFloat(pnl) >= 0 ? '🟢' : '🔴';

  await sendTelegram(
    `🔒 ─────────────────────\n` +
    `   ПОЗИЦІЯ ЗАКРИТА\n` +
    `─────────────────────\n` +
    `📄 Контракт: ${contract}\n` +
    `💵 Вхід:     ${entryPrice}\n` +
    `📊 Вихід:    ${markPrice}\n` +
    `${pnlEmoji} P&L:      $${pnl}\n` +
    `─────────────────────`,
    chatId
  );
}

async function cmdStatus(chatId) {
  await sendTelegram(
    `🤖 ─────────────────────\n` +
    `     СТАТУС БОТІВ\n` +
    `─────────────────────\n` +
    `🟢 Upbit Bot:   працює\n` +
    `🟢 Binance Bot: працює\n` +
    `─────────────────────\n` +
    `КОМАНДИ:\n` +
    `/balance — баланс\n` +
    `/pnl — позиції і P&L\n` +
    `/tp МОНЕТА ЦІНА — TP\n` +
    `/sl МОНЕТА ЦІНА — SL\n` +
    `/close МОНЕТА — закрити\n` +
    `/status — цей список\n` +
    `─────────────────────`,
    chatId
  );
}

// ─── Polling команд ───────────────────────────────────────────────────────────
let lastUpdateId = -1;

async function processCommands() {
  try {
    const agent = getAgent();
    const res = await axios.get(
      `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/getUpdates?offset=${lastUpdateId + 1}&timeout=5`,
      agent ? { httpsAgent: agent, timeout: 10000 } : { timeout: 10000 }
    );

    const updates = res.data.result || [];

    for (const update of updates) {
      if (update.update_id <= lastUpdateId) continue;
      lastUpdateId = update.update_id;
      const msg = update.message;
      if (!msg || !msg.text) continue;
      if (String(msg.chat.id) !== String(CONFIG.TELEGRAM_CHAT_ID)) continue;

      const parts = msg.text.trim().split(' ');
      const cmd = parts[0].toLowerCase();
      const chatId = msg.chat.id;

      console.log(`[CMD] ${msg.text}`);

      try {
        if (cmd === '/balance') {
          await cmdBalance(chatId);
        } else if (cmd === '/pnl') {
          await cmdPnl(chatId);
        } else if (cmd === '/tp' && parts.length === 3) {
          await cmdTp(parts[1].toUpperCase() + '_USDT', parseFloat(parts[2]), chatId);
        } else if (cmd === '/sl' && parts.length === 3) {
          await cmdSl(parts[1].toUpperCase() + '_USDT', parseFloat(parts[2]), chatId);
        } else if (cmd === '/close' && parts.length === 2) {
          await cmdClose(parts[1].toUpperCase() + '_USDT', chatId);
        } else if (cmd === '/status') {
          await cmdStatus(chatId);
        } else {
          await sendTelegram(`❓ Невідома команда.\nВведи /status для списку.`, chatId);
        }
      } catch (e) {
        await sendTelegram(`❌ Помилка: ${e.response?.data?.message || e.message}`, chatId);
      }
    }
  } catch (e) {
    console.error(`Commands error: ${e.message}`);
  }
}

async function initLastUpdateId() {
  try {
    const agent = getAgent();
    const res = await axios.get(
      `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/getUpdates?offset=-1`,
      agent ? { httpsAgent: agent, timeout: 10000 } : { timeout: 10000 }
    );
    const updates = res.data.result || [];
    if (updates.length > 0) {
      lastUpdateId = updates[updates.length - 1].update_id;
    }
    console.log(`Initialized lastUpdateId: ${lastUpdateId}`);
  } catch(e) {
    console.error(`Init error: ${e.message}`);
  }
}

async function main() {
  console.log('Telegram Commands Bot — starting');
  await initLastUpdateId();
  await sendTelegram(
    `🤖 ─────────────────────\n` +
    `  КОМАНДИ АКТИВОВАНІ!\n` +
    `─────────────────────\n` +
    `/balance — баланс\n` +
    `/pnl — позиції і P&L\n` +
    `/tp МОНЕТА ЦІНА — TP\n` +
    `/sl МОНЕТА ЦІНА — SL\n` +
    `/close МОНЕТА — закрити\n` +
    `/status — статус\n` +
    `─────────────────────`
  );

  setInterval(processCommands, 1000);
}

process.on('uncaughtException', e => {
  console.error(`Uncaught: ${e.message}`);
  sendTelegram(`❌ COMMANDS БОТ ВПАВ!\n${e.message}`);
  setTimeout(() => process.exit(1), 1000);
});

main().catch(e => {
  console.error(`Fatal: ${e.message}`);
  process.exit(1);
});
