require('dotenv').config({ path: '/root/bot/.env' });
const net = require('net');
const crypto = require('crypto');
const WebSocket = require('ws');
const fs = require('fs');

const KEY = process.env.BINANCE_API_KEY;
const SECRET = process.env.BINANCE_API_SECRET;
const SOCKET_PATH = '/tmp/binance_ws.sock';

let ws = null;
let connected = false;
let reqCounter = 0;
const pending = new Map();

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function signHmac(params) {
  const sorted = Object.keys(params).sort().map(k => `${k}=${params[k]}`).join('&');
  return crypto.createHmac('sha256', SECRET).update(sorted).digest('hex');
}

function connectBinance() {
  log('Connecting to Binance Futures WS...');
  ws = new WebSocket('wss://ws-fapi.binance.com/ws-fapi/v1');

  ws.on('open', () => {
    connected = true;
    log('Connected! ✅');
  });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      const id = msg.id;
      if (id && pending.has(id)) {
        const { resolve } = pending.get(id);
        pending.delete(id);
        resolve(msg);
      }
    } catch(e) {}
  });

  ws.on('close', () => {
    connected = false;
    log('Disconnected — reconnecting in 1s...');
    setTimeout(connectBinance, 1000);
  });

  ws.on('error', (e) => {
    log(`WS Error: ${e.message}`);
    connected = false;
  });

  // Pong на ping
  ws.on('ping', (data) => ws.pong(data));
}

function placeOrder(symbol, quantity, side = 'BUY') {
  return new Promise((resolve, reject) => {
    if (!connected || !ws) {
      return reject(new Error('Not connected'));
    }

    reqCounter++;
    const id = `order-${reqCounter}`;
    const ts = Date.now();

    const params = {
      apiKey: KEY,
      quantity: String(quantity),
      side: side.toUpperCase(),
      symbol: symbol.toUpperCase(),
      timestamp: ts,
      type: 'MARKET',
    };
    params.signature = signHmac(params);

    pending.set(id, { resolve, reject });

    ws.send(JSON.stringify({
      id,
      method: 'order.place',
      params
    }));

    setTimeout(() => {
      if (pending.has(id)) {
        pending.delete(id);
        reject(new Error('Timeout'));
      }
    }, 5000);
  });
}

// Unix socket сервер
if (fs.existsSync(SOCKET_PATH)) fs.unlinkSync(SOCKET_PATH);

const server = net.createServer((client) => {
  let data = '';
  client.on('data', async chunk => {
    data += chunk;
    try {
      const req = JSON.parse(data); // спробуємо парсити одразу
      const start = Date.now();
      const symbol = req.symbol || '';
      const quantity = req.quantity || 0;
      const side = req.side || 'BUY';

      log(`Order: ${side} ${quantity} ${symbol}`);

      const result = await placeOrder(symbol, quantity, side);
      const elapsed = Date.now() - start;

      const success = result.status === 200;
      const r = result.result || {};
      const fillPrice = String(r.avgPrice || r.price || '0');

      log(`Done: ${elapsed}ms success=${success} fill=${fillPrice}`);

      client.write(JSON.stringify({
        success,
        fill_price: fillPrice,
        elapsed_ms: elapsed,
        error: success ? '' : JSON.stringify(result.error || '')
      }), () => client.end());

    } catch(e) {
      if (e instanceof SyntaxError) return; // не повний JSON ще
      log(`Error: ${e.message}`);
      client.write(JSON.stringify({ success: false, error: e.message, fill_price: '0', elapsed_ms: 0 }), () => client.end());
    }
  });
  client.on('error', () => {});
});

server.listen(SOCKET_PATH, () => {
  fs.chmodSync(SOCKET_PATH, '0777');
  log(`Unix socket started: ${SOCKET_PATH}`);
});

connectBinance();

log('Starting Binance WS Service (Node.js)...');
