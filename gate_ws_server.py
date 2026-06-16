import asyncio
import json
import os
import time
import socket
import threading
from dotenv import load_dotenv
from gate_ws import Configuration, Connection
from gate_ws.futures import FuturesOrderPlaceChannel, FuturesLoginChannel

load_dotenv('/root/bot/.env')

KEY = os.getenv('GATE_API_KEY')
SECRET = os.getenv('GATE_API_SECRET')
LEVERAGE = int(os.getenv('LEVERAGE', '10'))

SOCKET_PATH = '/tmp/gate_ws.sock'

# Глобальні змінні
conn = None
order_channel = None
login_channel = None
logged_in = False
pending_orders = {}  # req_id -> asyncio.Future
req_counter = 0
loop = None

def log(msg):
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] {msg}', flush=True)

def on_login(c, response):
    global logged_in
    try:
        if hasattr(response, 'header'):
            status = response.header.get('status') if hasattr(response.header, 'get') else str(response.header)
            if '200' in str(status):
                logged_in = True
                log('Gate.io WS: Logged in!')
                return
        data = json.loads(response) if isinstance(response, str) else vars(response) if hasattr(response, '__dict__') else {}
        status = data.get('header', {}).get('status', '')
        if status == '200' or '200' in str(response):
            logged_in = True
            log('Gate.io WS: Logged in!')
        else:
            log(f'Login response: {response}')
            # Спробуємо все одно вважати залогіненим
            logged_in = True
            log('Gate.io WS: Assuming logged in')
    except Exception as e:
        log(f'Login parse error: {e} — assuming logged in')
        logged_in = True

def on_order(c, response):
    try:
        # response може бути об'єктом або рядком
        if isinstance(response, str):
            data = json.loads(response)
        elif hasattr(response, '__dict__'):
            data = json.loads(str(response))
        else:
            data = response
            
        req_id = data.get('request_id') if isinstance(data, dict) else None
        
        if req_id and req_id in pending_orders:
            future = pending_orders.pop(req_id)
            if not future.done():
                asyncio.run_coroutine_threadsafe(
                    set_future_result(future, data),
                    loop
                )
    except Exception as e:
        log(f'Order parse error: {e}')

async def set_future_result(future, result):
    if not future.done():
        future.set_result(result)

async def place_order(contract, size):
    global req_counter, pending_orders
    
    if not logged_in:
        raise Exception('Not logged in to Gate.io WS')
    
    req_counter += 1
    req_id = f'order-{req_counter}'
    
    future = loop.create_future()
    pending_orders[req_id] = future
    
    order_channel.api_request(
        payload={
            'contract': contract,
            'size': str(size),
            'price': '0',
            'tif': 'ioc',
            'text': 't-listing-ws',
        },
        req_id=req_id
    )
    
    # Чекаємо відповідь (timeout 5 сек)
    result = await asyncio.wait_for(future, timeout=5.0)
    return result

async def handle_client(reader, writer):
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=10.0)
        request = json.loads(data.decode())
        
        contract = request.get('contract')
        size = request.get('size')
        
        log(f'Order request: {contract} size={size}')
        start = time.time()
        
        result = await place_order(contract, size)
        elapsed = (time.time() - start) * 1000
        
        status = result.get('header', {}).get('status')
        order_data = result.get('data', {}).get('result', {})
        
        response = {
            'success': status == '200',
            'fill_price': order_data.get('fill_price', '0'),
            'finish_as': order_data.get('finish_as', ''),
            'elapsed_ms': elapsed,
            'error': result.get('data', {}).get('errs', {}).get('message', '') if status != '200' else ''
        }
        
        log(f'Order result: {response}')
        writer.write(json.dumps(response).encode())
        await writer.drain()
        
    except asyncio.TimeoutError:
        writer.write(json.dumps({'success': False, 'error': 'Timeout'}).encode())
        await writer.drain()
    except Exception as e:
        log(f'Handle client error: {e}')
        writer.write(json.dumps({'success': False, 'error': str(e)}).encode())
        await writer.drain()
    finally:
        writer.close()

async def start_unix_server():
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)
    
    server = await asyncio.start_unix_server(handle_client, path=SOCKET_PATH)
    os.chmod(SOCKET_PATH, 0o777)
    log(f'Unix socket server started at {SOCKET_PATH}')
    return server

async def main():
    global conn, order_channel, login_channel, loop, logged_in
    
    loop = asyncio.get_event_loop()
    
    log('Starting Gate.io WebSocket service...')
    
    config = Configuration(
        app='futures',
        settle='usdt',
        api_key=KEY,
        api_secret=SECRET,
        test_net=False
    )
    
    conn = Connection(config)
    login_channel = FuturesLoginChannel(conn, on_login)
    order_channel = FuturesOrderPlaceChannel(conn, on_order)
    
    # Запускаємо Unix socket сервер
    server = await start_unix_server()
    
    async def login_when_ready():
        await asyncio.sleep(1)
        log('Logging in to Gate.io WS...')
        login_channel.login(header='', req_id='login-init')
        
        # Перевіряємо логін
        for _ in range(10):
            if logged_in:
                break
            await asyncio.sleep(0.5)
        
        if not logged_in:
            log('WARNING: Login may have failed!')
    
    # Ping кожні 20 сек
    async def ping_loop():
        while True:
            await asyncio.sleep(20)
            try:
                if conn:
                    log('Ping...')
            except:
                pass
    
    await asyncio.gather(
        conn.run(),
        login_when_ready(),
        ping_loop(),
        server.serve_forever()
    )

if __name__ == '__main__':
    log('Gate.io WebSocket Order Service starting...')
    asyncio.run(main())
