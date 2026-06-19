import asyncio
import json
import os
import time
import hmac
import hashlib
import websockets
from dotenv import load_dotenv

load_dotenv('/root/bot/.env')

KEY = os.getenv('BINANCE_API_KEY')
SECRET = os.getenv('BINANCE_API_SECRET')
SOCKET_PATH = '/tmp/binance_ws.sock'

ws_conn = None
logged_in = False
pending_orders = {}
req_counter = 0
loop = None

def log(msg):
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] {msg}', flush=True)

def sign_hmac(params: dict) -> str:
    sorted_params = sorted(params.items())
    query = '&'.join(f'{k}={v}' for k, v in sorted_params)
    return hmac.new(SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()

async def connect_binance():
    global ws_conn, logged_in
    while True:
        try:
            log('Connecting to Binance Futures WS API...')
            async with websockets.connect('wss://ws-fapi.binance.com/ws-fapi/v1') as ws:
                ws_conn = ws
                log('Connected!')

                # Логін через session.logon з HMAC
                ts = int(time.time() * 1000)
                req_id = f'login-{ts}'
                params = {
                    'apiKey': KEY,
                    'timestamp': ts,
                }
                params['signature'] = sign_hmac(params)

                await ws.send(json.dumps({
                    'id': req_id,
                    'method': 'session.logon',
                    'params': params
                }))

                async for msg in ws:
                    data = json.loads(msg)

                    # Логін відповідь
                    if data.get('id', '').startswith('login'):
                        if data.get('status') == 200:
                            logged_in = True
                            log('Binance WS: Logged in! ✅')
                        else:
                            log(f'Login failed: {data.get("error", data)}')
                        continue

                    # Відповідь на ордер
                    resp_id = data.get('id', '')
                    if resp_id in pending_orders:
                        future = pending_orders.pop(resp_id)
                        if not future.done():
                            asyncio.run_coroutine_threadsafe(
                                set_result(future, data), loop
                            )

        except Exception as e:
            log(f'WS Error: {e} — reconnecting in 2s...')
            logged_in = False
            ws_conn = None
            await asyncio.sleep(2)

async def set_result(future, result):
    if not future.done():
        future.set_result(result)

async def place_order(symbol: str, quantity: float, side: str = 'BUY'):
    global req_counter
    if not logged_in or not ws_conn:
        raise Exception('Not logged in')

    req_counter += 1
    req_id = f'order-{req_counter}'
    future = loop.create_future()
    pending_orders[req_id] = future

    ts = int(time.time() * 1000)
    params = {
        'apiKey': KEY,
        'symbol': symbol.upper(),
        'side': side.upper(),
        'type': 'MARKET',
        'quantity': str(quantity),
        'timestamp': ts,
    }
    params['signature'] = sign_hmac(params)

    await ws_conn.send(json.dumps({
        'id': req_id,
        'method': 'order.place',
        'params': params
    }))

    result = await asyncio.wait_for(future, timeout=5.0)
    return result

async def handle_client(reader, writer):
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=10.0)
        request = json.loads(data.decode())

        symbol = request.get('symbol', '').upper()
        quantity = float(request.get('quantity', 0))
        side = request.get('side', 'BUY').upper()

        log(f'Order: {side} {quantity} {symbol}')
        start = time.time()

        result = await place_order(symbol, quantity, side)
        elapsed = (time.time() - start) * 1000

        status = result.get('status')
        success = status == 200
        fill_price = '0'

        if success:
            r = result.get('result', {})
            fill_price = str(r.get('avgPrice') or r.get('price') or '0')

        response = {
            'success': success,
            'fill_price': fill_price,
            'elapsed_ms': elapsed,
            'error': '' if success else str(result.get('error', ''))
        }

        log(f'Done: {elapsed:.0f}ms success={success} fill={fill_price}')
        writer.write(json.dumps(response).encode())
        await writer.drain()

    except Exception as e:
        log(f'Error: {e}')
        writer.write(json.dumps({'success': False, 'error': str(e)}).encode())
        await writer.drain()
    finally:
        writer.close()

async def start_server():
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)
    server = await asyncio.start_unix_server(handle_client, path=SOCKET_PATH)
    os.chmod(SOCKET_PATH, 0o777)
    log(f'Unix socket started: {SOCKET_PATH}')
    return server

async def check_login():
    while True:
        await asyncio.sleep(5)
        if not logged_in:
            log('Not logged in — waiting for reconnect...')

async def main():
    global loop
    loop = asyncio.get_event_loop()
    log('Starting Binance WS Service (HMAC)...')
    server = await start_server()
    await asyncio.gather(
        connect_binance(),
        check_login(),
        server.serve_forever()
    )

if __name__ == '__main__':
    asyncio.run(main())
