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
        resp_str = str(response)
        if '200' in resp_str and 'uid' in resp_str:
            logged_in = True
            log('Gate.io WS: Logged in! ✅')
        else:
            log(f'Login response: {resp_str[:100]}')
    except Exception as e:
        log(f'Login parse error: {e}')

def on_order(c, response):
    global pending_orders
    try:
        resp_str = str(response)
        data = json.loads(resp_str) if isinstance(response, str) else {}
        
        if hasattr(response, 'request_id'):
            req_id = response.request_id
        else:
            req_id = data.get('request_id')
        
        if req_id and req_id in pending_orders:
            future = pending_orders.pop(req_id)
            if not future.done():
                asyncio.run_coroutine_threadsafe(
                    set_result(future, response), loop
                )
    except Exception as e:
        log(f'Order parse error: {e}')

async def set_result(future, result):
    if not future.done():
        future.set_result(result)

async def place_order(contract, size):
    global req_counter
    if not logged_in:
        raise Exception('Not logged in')
    
    req_counter += 1
    req_id = f'order-{req_counter}-{int(time.time())}'
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
    
    result = await asyncio.wait_for(future, timeout=5.0)
    return result

async def handle_client(reader, writer):
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=10.0)
        request = json.loads(data.decode())
        contract = request.get('contract')
        size = request.get('size')
        
        log(f'Order: {contract} size={size} logged_in={logged_in}')
        start = time.time()
        
        result = await place_order(contract, size)
        elapsed = (time.time() - start) * 1000
        
        result_str = str(result)
        success = '200' in result_str
        
        fill_price = '0'
        try:
            if hasattr(result, 'data') and hasattr(result.data, 'result'):
                fill_price = str(result.data.result.get('fill_price', '0'))
        except:
            pass
        
        response = {
            'success': success,
            'fill_price': fill_price,
            'elapsed_ms': elapsed,
            'error': '' if success else result_str[:100]
        }
        
        log(f'Order result: {response}')
        writer.write(json.dumps(response).encode())
        await writer.drain()
        
    except asyncio.TimeoutError:
        writer.write(json.dumps({'success': False, 'error': 'Timeout'}).encode())
        await writer.drain()
    except Exception as e:
        log(f'Handle error: {e}')
        writer.write(json.dumps({'success': False, 'error': str(e)}).encode())
        await writer.drain()
    finally:
        writer.close()

async def start_server():
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)
    server = await asyncio.start_unix_server(handle_client, path=SOCKET_PATH)
    os.chmod(SOCKET_PATH, 0o777)
    log(f'Socket server started: {SOCKET_PATH}')
    return server

async def do_login():
    global logged_in
    logged_in = False
    log('Logging in...')
    login_channel.login(header='', req_id=f'login-{int(time.time())}')
    # Чекаємо логін до 10 сек
    for _ in range(20):
        await asyncio.sleep(0.5)
        if logged_in:
            log('Login confirmed! ✅')
            return True
    log('Login timeout!')
    return False

async def main():
    global conn, order_channel, login_channel, loop, logged_in
    loop = asyncio.get_event_loop()
    
    log('Gate.io WS Service starting...')
    
    server = await start_server()
    
    while True:
        try:
            log('Connecting to Gate.io WS...')
            logged_in = False
            
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
            
            async def run_with_login():
                await asyncio.sleep(1)
                success = await do_login()
                if not success:
                    log('Login failed — reconnecting...')
                    conn._ws.close() if hasattr(conn, '_ws') else None
                    return
                
                # Тримаємо логін живим
                while True:
                    await asyncio.sleep(5)
                    if not logged_in:
                        log('Lost login — re-logging in...')
                        await do_login()
            
            await asyncio.gather(
                conn.run(),
                run_with_login(),
                server.serve_forever()
            )
            
        except Exception as e:
            log(f'Connection error: {e} — reconnecting in 3s...')
            logged_in = False
            await asyncio.sleep(3)

if __name__ == '__main__':
    log('Starting...')
    asyncio.run(main())
