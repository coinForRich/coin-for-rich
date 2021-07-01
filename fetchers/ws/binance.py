### This script uses websocket to fetch binance 1-minute OHLCV data in real time

import asyncio
import json
import redis
import websockets
from common.config.constants import *
from fetchers.config.constants import WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY
from fetchers.rest.binance import BinanceOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.exceptions import UnsuccessfulConnection


URI = "wss://stream.binance.com:9443/ws"

class BinanceOHLCVWebsocket:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        self.ws_msg_ids = {
            "subscribe": 1
        }
        # Mapping from ws symbol to symbol
        # self.wssymbol_mapping = {}
    
    async def subscribe(self, symbols):
        async with websockets.connect(URI) as ws:
            # Binance requires WS symbols to be lowercase
            params = [
                f'{symbol.lower()}@kline_1m'
                for symbol in symbols
            ]
            # for symbol in symbols:
            #     wssymbol = f'{symbol.lower()}@kline_1m'
            #     self.wssymbol_mapping[wssymbol] = symbol
            #     params.append(wssymbol)
            await ws.send(json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": self.ws_msg_ids["subscribe"]
                }))
            while True:
                resp = await ws.recv()
                respj = json.loads(resp)
                try:
                    if isinstance(respj, dict):
                        print(respj)
                        if 'result' in respj:
                            if respj['result'] is not None:
                                raise UnsuccessfulConnection
                        else:
                            symbol = respj['s']
                            timestamp = respj['k']['t']
                            open = respj['k']['o']
                            high = respj['k']['h']
                            low = respj['k']['l']
                            close = respj['k']['c']
                            volume = respj['k']['v']
                except Exception as exc:
                    print(f"EXCEPTION: {exc}")
                await asyncio.sleep(1)

    async def mutual_basequote(self):
        # bitfinex_fetcher = BitfinexOHLCVFetcher()
        # bitfinex_fetcher.fetch_symbol_data()
        # symbols = bitfinex_fetcher.get_mutual_basequote()
        # bitfinex_fetcher.close_connections()
        symbols = ["ETHBTC"]
        await asyncio.gather(self.subscribe(symbols))
    
    def run_mutual_basequote(self):
        asyncio.run(self.mutual_basequote())