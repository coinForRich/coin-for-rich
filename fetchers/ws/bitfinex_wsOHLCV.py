### This script uses websocket to fetch bitfinex 1-minute OHLCV data in real time

import asyncio
import websockets
import time
import json


URI = "wss://api-pub.bitfinex.com/ws/2"
async def main():
    loop = asyncio.get_event_loop()
    tasks = []
    for i in range(1):
        tasks.append(loop.create_task(conn(i)))
    await asyncio.wait(tasks)

async def conn(i):
    async with websockets.connect(URI) as ws:
        msg = {'event': 'subscribe',  'channel': 'candles', 'key': 'trade:1m:tBTCUSD'}
        await ws.send(json.dumps(msg))
        while True:
            asyncio.sleep(0.25)
            resp = await ws.recv()
            print (f'Connection # {i} responded at time {round(time.time(),2)}')
            print(f'Response: {resp}')

def run():
    asyncio.run(main())

if __name__ == "__main__":
    run()


# async with websockets.connect(URI) as ws:
#     msg = {'event': 'subscribe',  'channel': 'candles', 'key': 'trade:1m:tBTCUSD'}
#     await ws.send(json.dumps(msg))
    
#     while True:
#         time.sleep(0.1)
#         resp = await ws.recv()
#         print(resp)