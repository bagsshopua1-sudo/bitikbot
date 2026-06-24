// protection.js — ставит SL/TP после открытия позиции. ОТДЕЛЬНЫЙ модуль.
// Не трогает открытие. Вызывается из бота: require('./protection').protect({...})
require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');

const K = process.env.GATE_API_KEY, S = process.env.GATE_API_SECRET;
const BASE = 'https://api.gateio.ws/api/v4';

function sign(method, endpoint, body) {
  const ts = Math.floor(Date.now()/1000).toString();
  const bh = crypto.createHash('sha512').update(body||'').digest('hex');
  const sig = crypto.createHmac('sha512', S).update(method+'\n/api/v4'+endpoint+'\n\n'+bh+'\n'+ts).digest('hex');
  return { ts, sig };
}

function req(method, endpoint, data) {
  const body = data ? JSON.stringify(data) : '';
  const { ts, sig } = sign(method, endpoint, body);
  return axios({ method, url: BASE+endpoint, headers: {'KEY':K,'SIGN':sig,'Timestamp':ts,'Content-Type':'application/json'}, data: data||undefined, timeout: 8000 }).then(r=>r.data);
}

// округление цены до price tick контракта
function roundToTick(price, tick) {
  const decimals = String(tick).split('.')[1]?.length || 0;
  return (Math.round(price/tick)*tick).toFixed(decimals);
}

// кеш price tick по контрактам
const tickCache = new Map();
async function getTick(contract) {
  if (tickCache.has(contract)) return tickCache.get(contract);
  try {
    const c = await axios.get(`${BASE}/futures/usdt/contracts/${contract}`, {timeout:5000}).then(r=>r.data);
    const tick = parseFloat(c.order_price_round);
    tickCache.set(contract, tick);
    return tick;
  } catch(e) { return 0.0001; }
}

// ГЛАВНАЯ функция. Вызывать ПОСЛЕ открытия позиции.
// opts: { contract, size, entryPrice, slPct, tpPct }
// size — размер позиции со знаком (+ лонг, - шорт)
// slPct/tpPct — проценты по ЦЕНЕ (0.02 = 2%, 0.14 = 14%)
async function protect(opts) {
  try {
    const { contract, size, entryPrice } = opts;
    const slPct = opts.slPct || 0.02;
    const tpPct = opts.tpPct || 0.14;
    const isLong = size > 0;
    const closeSize = -size; // противоположный знак для закрытия
    const tick = await getTick(contract);

    // SL: лонг падает (rule 2), шорт растёт (rule 1)
    const slRaw = isLong ? entryPrice*(1-slPct) : entryPrice*(1+slPct);
    const slPrice = roundToTick(slRaw, tick);
    // TP: лонг растёт (rule 1), шорт падает (rule 2)
    const tpRaw = isLong ? entryPrice*(1+tpPct) : entryPrice*(1-tpPct);
    const tpPrice = roundToTick(tpRaw, tick);

    // SL
    req('POST', '/futures/usdt/price_orders', {
      initial: { contract, size: closeSize, price:'0', tif:'ioc', reduce_only:true, text:'t-sl-auto' },
      trigger: { strategy_type:0, price_type:1, price: slPrice, rule: isLong?2:1, expiration:86400 }
    }).then(r=>console.log(`[protect] SL ${contract} @ ${slPrice} id ${r.id}`))
      .catch(e=>console.log(`[protect] SL fail: ${JSON.stringify(e.response?.data||e.message)}`));

    // TP
    req('POST', '/futures/usdt/price_orders', {
      initial: { contract, size: closeSize, price:'0', tif:'ioc', reduce_only:true, text:'t-tp-auto' },
      trigger: { strategy_type:0, price_type:1, price: tpPrice, rule: isLong?1:2, expiration:86400 }
    }).then(r=>console.log(`[protect] TP ${contract} @ ${tpPrice} id ${r.id}`))
      .catch(e=>console.log(`[protect] TP fail: ${JSON.stringify(e.response?.data||e.message)}`));

  } catch(e) {
    console.log(`[protect] error: ${e.message}`);
  }
}

module.exports = { protect };
