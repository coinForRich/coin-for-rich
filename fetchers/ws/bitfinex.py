### This script uses websocket to fetch bitfinex 1-minute OHLCV data in real time

import asyncio
import time
import json
import redis
import websockets
from common.config.constants import *
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher, EXCHANGE_NAME


URI = "wss://api-pub.bitfinex.com/ws/2"
SYMBOLS = ["ETHBTC"]
REDIS_KEY = "bitfinex;;tBTCUSD;;{timestamp}"

REDIS_CLIENT = redis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD)

async def main():
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.fetch_symbol_data()
    tasks = []
    for s in SYMBOLS:
        tasks.append(conn(s))
    await asyncio.gather(*tasks)

# TODO: start ws server to serve ws data from exchange
async def conn(symbol):
    tsymbol = BitfinexOHLCVFetcher.make_tsymbol(symbol)
    ws_symbol = f"trade:1m:{tsymbol}"
    async with websockets.connect(URI) as ws:
        msg = {'event': 'subscribe',  'channel': 'candles', 'key': ws_symbol}
        await ws.send(json.dumps(msg))
        while True:
            resp = await ws.recv()
            respj = json.loads(resp)
            try:
                if len(respj[1]) == 6:
                    print(f'Response: {respj}')
                    timestamp = respj[1][0]
                    open = respj[1][1]
                    high = respj[1][3]
                    low = respj[1][4]
                    close = respj[1][2]
                    volume = respj[1][5]
                    REDIS_CLIENT.hset(
                        f'{EXCHANGE_NAME}_{symbol}',
                        timestamp,
                        f'{timestamp}{REDIS_DELIMITER}{open}{REDIS_DELIMITER}{high}{REDIS_DELIMITER}{low}{REDIS_DELIMITER}{close}{REDIS_DELIMITER}{volume}'
                    )
            except Exception as exc:
                print(f'EXCEPTION: {exc}')

def run():
    asyncio.run(main())

if __name__ == "__main__":
    run()
