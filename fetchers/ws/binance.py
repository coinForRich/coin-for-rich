### This module uses websocket to fetch binance 1-minute OHLCV data in real time

import random
import asyncio
import json
import redis
import websockets
from typing import Iterable, NoReturn
from common.config.constants import (
    REDIS_HOST, REDIS_USER,
    REDIS_PASSWORD, REDIS_DELIMITER
)
from common.utils.logutils import create_logger
from fetchers.config.constants import WS_SUB_LIST_REDIS_KEY
from fetchers.config.queries import MUTUAL_BASE_QUOTE_QUERY
from fetchers.rest.binance import BinanceOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.exceptions import (
    UnsuccessfulConnection, ConnectionClosed, InvalidStatusCode
)
from fetchers.helpers.ws import (
    make_sub_val,
    make_sub_redis_key,
    make_send_redis_key
)


# Binance only allows up to 1024 subscriptions per ws connection
#   However, so far only a max value of 200 works...
URI = "wss://stream.binance.com:9443/ws"
MAX_SUB_PER_CONN = 200
MAX_SUB_PER_CONN = 25
BACKOFF_MIN_SECS = 2.0
BACKOFF_MAX_SECS = 60.0

class BinanceOHLCVWebsocket:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username=REDIS_USER,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        # self.ws_msg_ids = {
        #     "subscribe": 1
        # }

        # Rest fetcher for convenience
        self.rest_fetcher = BinanceOHLCVFetcher()

        # Logging
        self.logger = create_logger(f'{EXCHANGE_NAME}_websocket')

        # Backoff
        self.backoff_delay = BACKOFF_MIN_SECS

    async def subscribe(self, symbols: Iterable, i: int = 0) -> NoReturn:
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
                            "id": i
                        })
                    )
                    self.logger.info(f"Connection {i}: Successful")
                    self.backoff_delay = BACKOFF_MIN_SECS
                    while True:
                        resp = await ws.recv()
                        respj = json.loads(resp)
                            
                        if isinstance(respj, dict):
                            if 'result' in respj:
                                if respj['result'] is not None:
                                    raise UnsuccessfulConnection
                            else:
                                try:
                                    symbol = respj['s']
                                    timestamp = int(respj['k']['t'])
                                    open_ = respj['k']['o']
                                    high_ = respj['k']['h']
                                    low_ = respj['k']['l']
                                    close_ = respj['k']['c']
                                    volume_ = respj['k']['v']
                                    base_id = self.rest_fetcher.symbol_data[symbol]['base_id']
                                    quote_id = self.rest_fetcher.symbol_data[symbol]['quote_id']

                                    sub_val = make_sub_val(
                                        timestamp,
                                        open_, high_, low_, close_, volume_,
                                        REDIS_DELIMITER
                                    )

                                    ws_sub_redis_key = make_sub_redis_key(
                                        EXCHANGE_NAME,
                                        base_id,
                                        quote_id,
                                        REDIS_DELIMITER
                                    )
                                    ws_serve_redis_key = make_send_redis_key(
                                        EXCHANGE_NAME,
                                        base_id,
                                        quote_id,
                                        REDIS_DELIMITER
                                    )

                                    # Add ws sub key to set of all ws sub keys
                                    # Set hash value for ws sub key
                                    self.redis_client.sadd(
                                        WS_SUB_LIST_REDIS_KEY, ws_sub_redis_key)
                                    self.redis_client.hset(
                                        ws_sub_redis_key, timestamp, sub_val)
                                    current_timestamp = self.redis_client.hget(
                                        ws_serve_redis_key, 'time')
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
                                    self.logger.warning(
                                        f"Binance WS Fetcher: EXCEPTION: {exc}")

                        # Sleep to release event loop
                        await asyncio.sleep(0.01)
            except (ConnectionClosed, InvalidStatusCode) as exc:
                self.logger.warning(
                    f"Connection {i} raised exception: {exc} - reconnecting..."
                )
                await asyncio.sleep(min(self.backoff_delay, BACKOFF_MAX_SECS))
                self.backoff_delay *= (1+random.random()) # add a random factor

    async def mutual_basequote(self) -> None:
        symbols_dict = self.rest_fetcher.get_symbols_from_exch(MUTUAL_BASE_QUOTE_QUERY)
        self.rest_fetcher.close_connections()
        # symbols_dict = ["ETHBTC", "BTCEUR"]
        await asyncio.gather(self.subscribe(symbols_dict.keys()))

    async def all(self) -> None:
        '''
        Subscribes to WS channels of all symbols
        '''

        self.rest_fetcher.fetch_symbol_data()
        symbols =  tuple(self.rest_fetcher.symbol_data.keys())

        # Subscribe to `MAX_SUB_PER_CONN` per connection (e.g., 200)
        await asyncio.gather(
            *(
                self.subscribe(symbols[i:i+MAX_SUB_PER_CONN], int(i/MAX_SUB_PER_CONN))
                    for i in range(0, len(symbols), MAX_SUB_PER_CONN)
            )
        )
        # await asyncio.gather(self.subscribe(symbols))
    
    def run_mutual_basequote(self) -> None:
        asyncio.run(self.mutual_basequote())

    def run_all(self) -> None:
        asyncio.run(self.all())
