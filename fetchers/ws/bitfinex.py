### This module uses websocket to fetch bitfinex 1-minute OHLCV data in real time

import sys
import random
import logging
import asyncio
import json
import redis
import websockets
from typing import Any, Iterable, NoReturn
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, REDIS_DELIMITER
)
from common.utils.asyncioutils import AsyncLoopThread
from fetchers.config.constants import (
    WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY,
    WS_SUB_LIST_REDIS_KEY, WS_RATE_LIMIT_REDIS_KEY
)
from fetchers.config.queries import MUTUAL_BASE_QUOTE_QUERY
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.ratelimit import AsyncThrottler
from fetchers.utils.exceptions import (
    UnsuccessfulConnection, ConnectionClosed,
    InvalidStatusCode
)


# Bitfinex only allows up to 30 subscriptions per ws connection
URI = "wss://api-pub.bitfinex.com/ws/2"
MAX_SUB_PER_CONN = 25

class BitfinexOHLCVWebsocket:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        # Mapping from ws_symbol to symbol
        #   and mapping from channel ID to symbol
        self.wssymbol_mapping = {}
        self.chanid_mapping = {}

        # Rest fetcher for convenience
        self.rest_fetcher = BitfinexOHLCVFetcher()

        # Logging
        self.logger = logging.getLogger(f'{EXCHANGE_NAME}_websocket')
        self.logger.setLevel(logging.INFO)
        log_handler = logging.StreamHandler()
        log_handler.setLevel(logging.INFO)
        log_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(log_formatter)
        self.logger.addHandler(log_handler)

        # Rate limit manager
        self.rate_limiter = AsyncThrottler(
            WS_RATE_LIMIT_REDIS_KEY.format(exchange = EXCHANGE_NAME),
            1,
            3,
            redis_client = self.redis_client
        )

        # Loop
        # self.loop_handler = AsyncLoopThread(daemon=None)
        # self.loop_handler.start()

    async def subscribe_one(
        self,
        symbol: str,
        ws_client: Any
    ) -> None:
        '''
        Connects to WS endpoint for a symbol

        :params:
            `symbol`: string of symbol
            `ws_client`: websockets client obj
        '''

        tsymbol = self.rest_fetcher.make_tsymbol(symbol)
        ws_symbol = f"trade:1m:{tsymbol}"
        self.wssymbol_mapping[ws_symbol] = symbol
        msg = {'event': 'subscribe',  'channel': 'candles', 'key': ws_symbol}
        await ws_client.send(json.dumps(msg))

    async def subscribe(self, symbols: Iterable, i: int = 0) -> NoReturn:
        '''
        Subscribes to Bitfinex WS for `symbols`

        :params:
            `symbols` list of symbols
                e.g., ['ETHBTC', 'BTCEUR']
        '''

        while True:
            try:
                # Delay before making a connection
                async with self.rate_limiter:
                    async with websockets.connect(URI) as ws:
                        await asyncio.gather(
                            *(self.subscribe_one(symbol, ws) for symbol in symbols))
                        self.logger.info(f"Connection {i}: Successful")
                        while True:
                            resp = await ws.recv()
                            respj = json.loads(resp)
                            try:
                                # If resp is dict, find the symbol using wssymbol_mapping
                                #   and then map chanID to found symbol
                                # If resp is list, make sure its length is 6
                                #   and use the mappings to find symbol and push to Redis
                                if isinstance(respj, dict):
                                    if 'event' in respj:
                                        if respj['event'] != "subscribed":
                                            raise UnsuccessfulConnection
                                        else:
                                            symbol = self.wssymbol_mapping[respj['key']]
                                            self.chanid_mapping[respj['chanId']] = symbol
                                if isinstance(respj, list):
                                    if len(respj[1]) == 6:
                                        # self.logger.info(f"Response: {respj}")
                                        symbol = self.chanid_mapping[respj[0]]
                                        timestamp = int(respj[1][0])
                                        open_ = respj[1][1]
                                        high_ = respj[1][3]
                                        low_ = respj[1][4]
                                        close_ = respj[1][2]
                                        volume_ = respj[1][5]
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
                                            quote_id = quote_id
                                        )
                                        ws_serve_redis_key = WS_SERVE_REDIS_KEY.format(
                                            exchange = EXCHANGE_NAME,
                                            delimiter = REDIS_DELIMITER,
                                            base_id = base_id,
                                            quote_id = quote_id
                                        )

                                        # logging.info(f'ws sub redis key: {ws_sub_redis_key}')
                                        # logging.info(f'ws serve redis key: {ws_serve_redis_key}')
                                        
                                        # Add ws sub key to set of all ws sub keys
                                        # Set hash value for ws sub key
                                        # Replace ws serve key hash if this timestamp
                                        #   is more up-to-date
                                        self.redis_client.sadd(
                                            WS_SUB_LIST_REDIS_KEY, ws_sub_redis_key)
                                        self.redis_client.hset(
                                            ws_sub_redis_key, timestamp, sub_val)
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
                                self.logger.warning(f"EXCEPTION: {exc}")
                            await asyncio.sleep(0.1)
            except ConnectionClosed as exc:
                self.logger.warning(
                    f"Connection {i} closed with reason: {exc} - reconnecting..."
                )
            except InvalidStatusCode as exc:
                self.logger.warning(
                    f"Connection {i} returns invalid status code: {exc} - sleeping and reconnecting..."
                )
                await asyncio.sleep(5)
            
            # except Exception as exc:
            #     self.logger.warning(f"EXCEPTION in connection {i}: {exc}")
            #     raise Exception(exc)

    async def mutual_basequote(self) -> None:
        '''
        Subscribes to WS channels of the mutual symbols
            among all exchanges
        '''

        symbols_dict = self.rest_fetcher.get_symbols_from_exch(MUTUAL_BASE_QUOTE_QUERY)
        self.rest_fetcher.close_connections()
        # symbols = ["ETHBTC", "BTCEUR"]
        await asyncio.gather(self.subscribe(symbols_dict.keys()))

    async def all(self) -> None:
    # def all(self):
        '''
        Subscribes to WS channels of all symbols
        '''

        self.rest_fetcher.fetch_symbol_data()
        symbols =  tuple(self.rest_fetcher.symbol_data.keys())

        # TODO: probably running in different threads is not needed
        # for i in range(0, len(symbols), MAX_SUB_PER_CONN):
        #     asyncio.run_coroutine_threadsafe(
        #         self.subscribe(symbols[i:i+MAX_SUB_PER_CONN], i),
        #         # self.coroutine(5, i),
        #         self.loop_handler.loop
        #     )

        # Subscribe to `MAX_SUB_PER_CONN` per connection (e.g., 30)
        await asyncio.gather(
            *(
                self.subscribe(symbols[i:i+MAX_SUB_PER_CONN], int(i/MAX_SUB_PER_CONN))
                    for i in range(0, len(symbols), MAX_SUB_PER_CONN)
            )
        )
            
    def run_mutual_basequote(self) -> None:
        asyncio.run(self.mutual_basequote())

    def run_all(self) -> None:
        asyncio.run(self.all())
        # self.all()
