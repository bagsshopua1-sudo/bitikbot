require('dotenv').config();
const WebSocket = require('ws');
const crypto = require('crypto');

const KEY = process.env.GATE_API_KEY;
const SECRET = process.env.GATE_API_SECRET;

const ws = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');

ws.on('open', () => {
  console.log('Connected!');
  const ts  = Math.floor(Date.now() / 1000);
  const sig = crypto.createHmac('sha512', SECRET).update(`api\n${ts}`).digest('hex');
  
  const msg = {
    time:    ts,
    channel: 'futures.login',
    event:   'api',
    payload: {
      api_key:   KEY,
      signature: sig,
      timestamp: String(ts),
    },
  };
  console.log('Sending auth:', JSON.stringify(msg));
  ws.send(JSON.stringify(msg));
});

ws.on('message', d => {
  console.log('MSG:', d.toString());
});

ws.on('error', e => console.log('Error:', e.message));
ws.on('close', (code, reason) => console.log('Closed:', code, reason.toString()));

setTimeout(() => { console.log('Timeout'); ws.close(); }, 10000);
