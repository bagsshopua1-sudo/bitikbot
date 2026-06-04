require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');

const CONFIG = {
  GATE_API_KEY:     process.env.GATE_API_KEY,
  GATE_API_SECRET:  process.env.GATE_API_SECRET,
  TELEGRAM_TOKEN:   process.env.TELEGRAM_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
  LEVERAGE:         parseInt(process.env.LEVERAGE || '10'),
  GATE_BASE:        'https://api.gateio.ws/api/v4',
};

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

async function sendTelegram(text) {
  await axios.post(
    `https://api.telegram.org/bot${CONFIG.TELEGRAM_TOKEN}/sendMessage`,
    { chat_id: CONFIG.TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }
  );
}

async function test() {
  console.log('Получаем данные с Upbit API...\n');

  // Шаг 1: Берём реальный список монет с Upbit
  const res = await axios.get('https://crix-static.upbit.com/v2/crix_master', {
    headers: {
      'User-Agent': 'Mozilla/5.0',
      'Accept': 'application/json',
      'Origin': 'https://upbit.com',
      'Referer': 'https://upbit.com/',
    }
  });

  const coins = res.data;
  console.log(`✅ Получено монет с Upbit: ${coins.length}`);

  // Шаг 2: Находим SLX в реальных данных Upbit
  const slx = coins.find(c => c.baseCurrencyCode === 'SLX' && c.quoteCurrencyCode === 'KRW');
  
  if (!slx) {
    console.log('❌ SLX не найден на Upbit');
    return;
  }

  console.log(`✅ Нашли SLX на Upbit:`);
  console.log(`   Название: ${slx.englishName}`);
  console.log(`   Дата листинга: ${slx.listingDate}`);
  console.log(`   Статус: ${slx.marketState}`);

  const seenAt = Date.now();
  const ticker = 'SLX';
  const contract = `${ticker}_USDT`;

  // Шаг 3: Проверяем есть ли контракт на Gate.io
  console.log(`\nПроверяем контракт ${contract} на Gate.io...`);
  let markPrice, quanto;
  try {
    const contractInfo = await axios.get(`${CONFIG.GATE_BASE}/futures/usdt/contracts/${contract}`);
    markPrice = parseFloat(contractInfo.data.mark_price);
    quanto = parseFloat(contractInfo.data.quanto_multiplier || '1');
    console.log(`✅ Контракт найден! Цена: ${markPrice} USDT`);
  } catch(e) {
    console.log(`❌ Контракт ${contract} не найден на Gate.io`);
    await sendTelegram(
      `🧪 <b>ТЕСТ SLX</b>\n\n` +
      `✅ SLX найден на Upbit (листинг: ${slx.listingDate})\n` +
      `❌ Контракт SLX_USDT не найден на Gate.io Futures\n\n` +
      `ℹ️ Бот работает правильно — просто SLX нет на Gate.io`
    );
    return;
  }

  // Шаг 4: Баланс
  const account = await gateRequest('GET', '/futures/usdt/accounts', null, null);
  const available = parseFloat(account.available);
  console.log(`✅ Баланс: ${available} USDT`);

  // Шаг 5: Кросс-плечо
  await gateRequest('POST', `/futures/usdt/positions/${contract}/leverage`,
    { leverage: '0', cross_leverage_limit: String(CONFIG.LEVERAGE) }, null);
  console.log(`✅ Плечо ${CONFIG.LEVERAGE}x установлено`);

  // Шаг 6: Размер позиции
  const useMargin = available * 0.9;
  const size = Math.max(1, Math.floor((useMargin * CONFIG.LEVERAGE) / (markPrice * quanto)));
  const posValue = (size * markPrice * quanto).toFixed(2);
  console.log(`✅ Размер: ${size} контрактов (~$${posValue})`);

  // Шаг 7: Открываем ордер
  const order = await gateRequest('POST', '/futures/usdt/orders', null, {
    contract, size, price: '0', tif: 'ioc', text: 't-test',
  });

  const entryPrice = parseFloat(order.fill_price) || markPrice;
  const elapsedSec = ((Date.now() - seenAt) / 1000).toFixed(2);

  console.log(`\n🚀 ОРДЕР ОТКРЫТ!`);
  console.log(`Цена входа: ${entryPrice}`);
  console.log(`Скорость: ${elapsedSec} сек`);

  await sendTelegram(
    `🧪 <b>ТЕСТ ЛИСТИНГА — SLX</b>\n\n` +
    `📌 Монета: <b>SLX (Solstice)</b>\n` +
    `📅 Дата листинга на Upbit: <b>${slx.listingDate}</b>\n` +
    `📄 Контракт Gate.io: <code>${contract}</code>\n` +
    `💵 Цена входа: <b>${entryPrice} USDT</b>\n` +
    `📊 Размер: <b>$${posValue}</b>\n` +
    `⚡️ Плечо: <b>${CONFIG.LEVERAGE}x (кросс)</b>\n` +
    `⏱ Скорость: <b>${elapsedSec} сек</b>\n\n` +
    `✅ Бот работает как надо!`
  );

  console.log(`✅ Уведомление в Telegram отправлено!`);
  console.log(`\n⚠️ Закрой позицию вручную на Gate.io!`);
}

test().catch(e => console.log('ОШИБКА:', e.response?.data || e.message));
