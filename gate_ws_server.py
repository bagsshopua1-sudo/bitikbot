import asyncio
import json
import os
import time
from dotenv import load_dotenv
from gate_ws import Configuration, Connection
from gate_ws.futures import FuturesOrderPlaceChannel, FuturesLoginChannel

load_dotenv('/root/bot/.env')

KEY = os.getenv('GATE_API_KEY')
SECRET = os.getenv('GATE_API_SECRET')
SOCKET_PATH = '/tmp/gate_ws.sock'

logged_in = False
conn = None
order_channel = None
login_channel = None
pending_orders = {}
req_counter = 0
loop = None

def log(msg):
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] {msg}', flush=True)

def on_login(c, response):
    global logged_in
    try:
        logged_in = True
        log('Gate.io WS: Logged in!')
    except Exception as e:
        log(f'Login error: {e}')
        logged_in = True

ack_received = set()  # req_id які вже отримали ACK

def on_order(c, response):
    global pending_orders, ack_received
    try:
        if not pending_orders:
            return
        resp_str = str(response)
        
        # Знаходимо req_id з відповіді
        req_id = None
        for key in list(pending_orders.keys()):
            if key in resp_str:
                req_id = key
                break
        
        if not req_id:
            req_id = list(pending_orders.keys())[0]
        
        # Якщо це ACK (перша відповідь) — чекаємо другу
        if req_id in resp_str and 'req_param' in resp_str:
            log(f'[on_order] ACK for {req_id}, waiting for result...')
            ack_received.add(req_id)
            return
        
        # Це реальний результат ордера
        if req_id in pending_orders:
            log(f'[on_order] Result for {req_id}: {resp_str[:100]}')
            future = pending_orders.pop(req_id)
            ack_received.discard(req_id)
            if not future.done():
                asyncio.run_coroutine_threadsafe(set_result(future, response), loop)
    except Exception as e:
        log(f'Order error: {e}')

async def set_result(future, result):
    if not future.done():
        future.set_result(result)

async def place_order(contract, size):
    global req_counter
    if not logged_in:
        raise Exception('Not logged in')
    req_counter += 1
    req_id = f'order-{req_counter}'
    future = loop.create_future()
    pending_orders[req_id] = future
    order_channel.api_request(
        payload={'contract': contract, 'size': str(size), 'price': '0', 'tif': 'ioc', 'text': 't-listing-ws'},
        req_id=req_id
    )
    result = await asyncio.wait_for(future, timeout=5.0)
    return result

async def handle_client(reader, writer):
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=10.0)
        request = json.loads(data.decode())
        contract = request.get('contract')
        size = request.get('size')
        log(f'Order: {contract} size={size}')
        start = time.time()
        result = await place_order(contract, size)
        elapsed = (time.time() - start) * 1000
        result_str = str(result)
        # success тільки якщо є реальний результат ордера з id
        success = '200' in result_str and ('fill_price' in result_str or '"id":' in result_str) and 'req_param' not in result_str
        fill_price = '0'
        try:
            resp_str = str(result)
            import re
            match = re.search(r'"fill_price":"([^"]+)"', resp_str)
            if match:
                fill_price = match.group(1)
        except:
            pass
        response = {'success': success, 'fill_price': fill_price, 'elapsed_ms': elapsed, 'error': ''}
        log(f'Done: {elapsed:.0f}ms success={success}')
        writer.write(json.dumps(response).encode())
        await writer.drain()
    except asyncio.TimeoutError:
        writer.write(json.dumps({'success': False, 'error': 'Timeout'}).encode())
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
    log(f'Socket server started')
    return server

async def main():
    global conn, order_channel, login_channel, loop, logged_in
    loop = asyncio.get_event_loop()
    log('Starting Gate.io WS Service...')
    config = Configuration(app='futures', settle='usdt', api_key=KEY, api_secret=SECRET, test_net=False)
    conn = Connection(config)
    login_channel = FuturesLoginChannel(conn, on_login)
    order_channel = FuturesOrderPlaceChannel(conn, on_order)
    server = await start_server()

    async def login_loop():
        await asyncio.sleep(1)
        while not logged_in:
            log('Logging in...')
            login_channel.login(header='', req_id=f'login-{int(time.time())}')
            await asyncio.sleep(3)
        log('Login confirmed!')
        # Перевірка кожні 5 сек
        while True:
            await asyncio.sleep(5)
            if not logged_in:
                log('Re-logging in...')
                login_channel.login(header='', req_id=f'relogin-{int(time.time())}')
                await asyncio.sleep(3)

    await asyncio.gather(conn.run(), login_loop(), server.serve_forever())

if __name__ == '__main__':
    log('Gate.io WebSocket Order Service starting...')
    asyncio.run(main())
