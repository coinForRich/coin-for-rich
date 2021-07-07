### This script uses websocket to fetch binance 1-minute OHLCV data in real time

import asyncio
import json
import redis
import websockets
from common.config.constants import *
from fetchers.config.constants import WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY
from fetchers.rest.binance import BinanceOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.exceptions import UnsuccessfulConnection, ConnectionClosedOK


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
    
    async def subscribe(self, symbols):
        '''
        Subscribes to Binance WS for `symbols`
        :params:
            `symbols`: list of symbols (not tsymbol)
                e.g., [ETHBTC]
        '''

        while True:
            try:
                async with websockets.connect(URI) as ws:
                    # Binance requires WS symbols to be lowercase
                    params = [
                        f'{symbol.lower()}@kline_1m'
                        for symbol in symbols
                    ]
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
                                if 'result' in respj:
                                    if respj['result'] is not None:
                                        raise UnsuccessfulConnection
                                else:
                                    print(f'Response: {respj}')
                                    symbol = respj['s']
                                    timestamp = respj['k']['t']
                                    open = respj['k']['o']
                                    high = respj['k']['h']
                                    low = respj['k']['l']
                                    close = respj['k']['c']
                                    volume = respj['k']['v']
                                    sub_val = f'{timestamp}{REDIS_DELIMITER}{open}{REDIS_DELIMITER}{high}{REDIS_DELIMITER}{low}{REDIS_DELIMITER}{close}{REDIS_DELIMITER}{volume}'

                                    # Setting Redis data for updating ohlcv psql db
                                    #   and serving real-time chart
                                    # This Redis-update-ohlcv-psql-db-procedure
                                    #   may be changed with a pipeline from fastAPI...
                                    ws_sub_redis_key = WS_SUB_REDIS_KEY.format(
                                        exchange = EXCHANGE_NAME,
                                        symbol = symbol)
                                    ws_serve_redis_key = WS_SERVE_REDIS_KEY.format(
                                        exchange = EXCHANGE_NAME,
                                        symbol = symbol)
                                    self.redis_client.hset(ws_sub_redis_key, timestamp, sub_val)
                                    self.redis_client.hset(
                                        ws_serve_redis_key,
                                        mapping = {
                                            'time': timestamp,
                                            'open': open,
                                            'high': high,
                                            'low': low,
                                            'close': close,
                                            'volume': volume
                                        }
                                    )

                        except Exception as exc:
                            print(f"EXCEPTION: {exc}")
            except ConnectionClosedOK:
                pass
            except Exception as exc:
                print(f"EXCEPTION: {exc}")

    async def mutual_basequote(self):
        # bitfinex_fetcher = BitfinexOHLCVFetcher()
        # bitfinex_fetcher.fetch_symbol_data()
        # symbols = bitfinex_fetcher.get_mutual_basequote()
        # bitfinex_fetcher.close_connections()
        symbols = ["ETHBTC", "BTCEUR"]
        await asyncio.gather(self.subscribe(symbols))
    
    def run_mutual_basequote(self):
        asyncio.run(self.mutual_basequote())