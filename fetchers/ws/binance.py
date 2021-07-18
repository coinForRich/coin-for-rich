### This module uses websocket to fetch binance 1-minute OHLCV data in real time

import time
import asyncio
import json
import redis
import websockets
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, REDIS_DELIMITER
)
from fetchers.config.constants import (
    WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY, WS_SUB_LIST_REDIS_KEY
)
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

        # Rest fetcher for convenience
        self.rest_fetcher = BinanceOHLCVFetcher()
    
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
                        })
                    )
                    while True:
                        resp = await ws.recv()
                        respj = json.loads(resp)
                        try:
                            if isinstance(respj, dict):
                                if 'result' in respj:
                                    if respj['result'] is not None:
                                        raise UnsuccessfulConnection
                                else:
                                    print(f'At time {round(time.time(), 2)}, response: {respj}')
                                    symbol = respj['s']
                                    timestamp = int(respj['k']['t'])
                                    open_ = respj['k']['o']
                                    high_ = respj['k']['h']
                                    low_ = respj['k']['l']
                                    close_ = respj['k']['c']
                                    volume_ = respj['k']['v']
                                    sub_val = f'{timestamp}{REDIS_DELIMITER}{open_}{REDIS_DELIMITER}{high_}{REDIS_DELIMITER}{low_}{REDIS_DELIMITER}{close_}{REDIS_DELIMITER}{volume_}'

                                    # Setting Redis data for updating ohlcv psql db
                                    #   and serving real-time chart
                                    # This Redis-update-ohlcv-psql-db-procedure
                                    #   may be changed with a pipeline from fastAPI...
                                    base_id = self.rest_fetcher.symbol_data[symbol]['base_id']
                                    quote_id = self.rest_fetcher.symbol_data[symbol]['quote_id']
                                    ws_sub_redis_key = WS_SUB_REDIS_KEY.format(
                                        exchange = EXCHANGE_NAME,
                                        delimiter = REDIS_DELIMITER,
                                        base_id = base_id,
                                        quote_id = quote_id)
                                    ws_serve_redis_key = WS_SERVE_REDIS_KEY.format(
                                        exchange = EXCHANGE_NAME,
                                        delimiter = REDIS_DELIMITER,
                                        base_id = base_id,
                                        quote_id = quote_id)

                                    print(f'ws sub redis key: {ws_sub_redis_key}')
                                    print(f'ws serve redis key: {ws_serve_redis_key}')

                                    # Add ws sub key to set of all ws sub keys
                                    # Set hash value for ws sub key
                                    self.redis_client.sadd(
                                        WS_SUB_LIST_REDIS_KEY, ws_sub_redis_key
                                    )
                                    self.redis_client.hset(
                                        ws_sub_redis_key, timestamp, sub_val
                                    )
                                    current_timestamp = self.redis_client.hget(
                                        ws_serve_redis_key,
                                        'time')
                                    if current_timestamp is None or \
                                        timestamp >= int(current_timestamp):
                                        self.redis_client.hset(
                                            ws_serve_redis_key,
                                            mapping = {
                                                'time': timestamp,
                                                'open': open_,
                                                'high': high_,
                                                'low': low_,
                                                'close': close_,
                                                'volume': volume_
                                            }
                                        )
                        except Exception as exc:
                            print(f"EXCEPTION: {exc}")
            except ConnectionClosedOK:
                pass
            except Exception as exc:
                print(f"EXCEPTION: {exc}")

    async def mutual_basequote(self):
        symbols_dict = self.rest_fetcher.get_mutual_basequote()
        self.rest_fetcher.close_connections()
        # symbols_dict = ["ETHBTC", "BTCEUR"]
        await asyncio.gather(self.subscribe(symbols_dict.keys()))
    
    def run_mutual_basequote(self):
        asyncio.run(self.mutual_basequote())