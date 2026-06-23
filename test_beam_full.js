require('dotenv').config();
const net = require('net');
const axios = require('axios');
const crypto = require('crypto');

const GK = process.env.GATE_API_KEY, GS = process.env.GATE_API_SECRET;
const BK = process.env.BINANCE_API_KEY, BS = process.env.BINANCE_API_SECRET;

function signGate(m,e,b){const ts=Math.floor(Date.now()/1000).toString();const bh=crypto.createHash('sha512').update(b||'').digest('hex');const sig=crypto.createHmac('sha512',GS).update(m+'\n/api/v4'+e+'\n\n'+bh+'\n'+ts).digest('hex');return{ts,sig};}
function gateReq(m,e,b){const body=b?JSON.stringify(b):'';const{ts,sig}=signGate(m,e,body);return axios({method:m,url:'https://api.gateio.ws/api/v4'+e,headers:{'KEY':GK,'SIGN':sig,'Timestamp':ts,'Content-Type':'application/json'},data:b||undefined,timeout:8000}).then(r=>r.data);}
function signBin(p){return crypto.createHmac('sha256',BS).update(Object.keys(p).sort().map(k=>k+'='+p[k]).join('&')).digest('hex');}

const TICKER='BEAM';
const GC=TICKER+'_USDT', BSY=TICKER+'USDT';

async function main(){
  console.log('=== ТЕСТ ПОЛНОЙ ЦЕПОЧКИ '+TICKER+' (Gate + Binance) ===\n');

  // ── GATE.IO ──
  console.log('--- GATE.IO ---');
  // плечо
  await gateReq('POST','/futures/usdt/positions/'+GC+'/leverage',{leverage:'0',cross_leverage_limit:'15'}).then(()=>console.log('плечо 15x OK')).catch(e=>{const msg=e.response?.data?.message||e.message;const m=msg.match(/\[1,\s*(\d+)\]/);if(m){return gateReq('POST','/futures/usdt/positions/'+GC+'/leverage',{leverage:'0',cross_leverage_limit:m[1]}).then(()=>console.log('плечо max '+m[1]+'x OK'));}console.log('плечо:',msg);});

  // открытие через WS сокет
  const t0=Date.now();
  const gateOpen=await new Promise((res,rej)=>{
    const c=net.createConnection('/tmp/gate_ws.sock',()=>c.write(JSON.stringify({contract:GC,size:1})));
    let d='';c.on('data',x=>d+=x);c.on('end',()=>{try{res(JSON.parse(d));}catch(e){rej(e);}});c.on('error',rej);
    setTimeout(()=>{c.destroy();rej(new Error('timeout'));},6000);
  }).catch(e=>({success:false,error:e.message}));
  console.log('открытие:',Date.now()-t0,'ms | success:',gateOpen.success,'| fill:',gateOpen.fill_price);

  if(gateOpen.success && gateOpen.fill_price && gateOpen.fill_price!=='0'){
    const entry=parseFloat(gateOpen.fill_price);
    const slPrice=(entry*0.98).toFixed(8), tpPrice=(entry*1.14).toFixed(8);
    // SL
    await gateReq('POST','/futures/usdt/price_orders',{initial:{contract:GC,size:-1,price:'0',tif:'ioc',reduce_only:true,text:'t-sl'},trigger:{strategy_type:0,price_type:1,price:slPrice,rule:2,expiration:86400}}).then(r=>console.log('SL @',slPrice,'id',r.id)).catch(e=>console.log('SL fail:',e.response?.data?.label||e.message));
    // TP
    await gateReq('POST','/futures/usdt/price_orders',{initial:{contract:GC,size:-1,price:'0',tif:'ioc',reduce_only:true,text:'t-tp'},trigger:{strategy_type:0,price_type:1,price:tpPrice,rule:1,expiration:86400}}).then(r=>console.log('TP @',tpPrice,'id',r.id)).catch(e=>console.log('TP fail:',e.response?.data?.label||e.message));
    console.log('трейлинг 1.5% запустился бы через 500мс (в боте)');
  }

  // ── BINANCE ──
  console.log('\n--- BINANCE ---');
  // проверяем есть ли контракт
  const exInfo=await axios.get('https://fapi.binance.com/fapi/v1/exchangeInfo').then(r=>r.data.symbols.find(s=>s.symbol===BSY)).catch(()=>null);
  if(!exInfo){console.log('Контракта',BSY,'нет на Binance Futures — пропускаем (норм для свежих листингов)');return;}

  // плечо
  const lts=Date.now();
  const lp={symbol:BSY,leverage:15,timestamp:lts};lp.signature=signBin(lp);
  await axios.post('https://fapi.binance.com/fapi/v1/leverage?'+Object.keys(lp).sort().map(k=>k+'='+lp[k]).join('&'),null,{headers:{'X-MBX-APIKEY':BK}}).then(()=>console.log('плечо 15x OK')).catch(e=>console.log('плечо:',e.response?.data?.msg||e.message));

  // цена для расчёта qty
  const price=await axios.get('https://fapi.binance.com/fapi/v1/ticker/price?symbol='+BSY).then(r=>parseFloat(r.data.price));
  const qty=Math.max(1,Math.ceil(6/price)); // ~6$ позиция (мин notional 5$)
  console.log('цена',price,'qty',qty);

  // открытие MARKET
  const ots=Date.now();
  const op={symbol:BSY,side:'BUY',type:'MARKET',quantity:qty,timestamp:ots};op.signature=signBin(op);
  const binOpen=await axios.post('https://fapi.binance.com/fapi/v1/order?'+Object.keys(op).sort().map(k=>k+'='+op[k]).join('&'),null,{headers:{'X-MBX-APIKEY':BK}}).then(r=>r.data).catch(e=>({err:e.response?.data?.msg||e.message}));
  if(binOpen.err){console.log('открытие fail:',binOpen.err);return;}
  console.log('открытие OK | orderId',binOpen.orderId,'| статус',binOpen.status);

  // TP +7% SL -2%
  const bEntry=price;
  const bSL=(bEntry*0.98).toFixed(price<1?6:2), bTP=(bEntry*1.07).toFixed(price<1?6:2);
  const slp={symbol:BSY,side:'SELL',type:'STOP_MARKET',stopPrice:bSL,closePosition:'true',timestamp:Date.now()};slp.signature=signBin(slp);
  await axios.post('https://fapi.binance.com/fapi/v1/order?'+Object.keys(slp).sort().map(k=>k+'='+slp[k]).join('&'),null,{headers:{'X-MBX-APIKEY':BK}}).then(()=>console.log('SL @',bSL)).catch(e=>console.log('SL fail:',e.response?.data?.msg||e.message));
  const tpp={symbol:BSY,side:'SELL',type:'TAKE_PROFIT_MARKET',stopPrice:bTP,closePosition:'true',timestamp:Date.now()};tpp.signature=signBin(tpp);
  await axios.post('https://fapi.binance.com/fapi/v1/order?'+Object.keys(tpp).sort().map(k=>k+'='+tpp[k]).join('&'),null,{headers:{'X-MBX-APIKEY':BK}}).then(()=>console.log('TP @',bTP)).catch(e=>console.log('TP fail:',e.response?.data?.msg||e.message));
}
main().catch(e=>console.log('ERR',e.message));
