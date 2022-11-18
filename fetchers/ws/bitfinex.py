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
    REDIS_HOST, REDIS_USER,
    REDIS_PASSWORD, REDIS_DELIMITER
)
from common.utils.logutils import create_logger
from common.utils.asyncioutils import AsyncLoopThread
from fetchers.config.constants import (
    WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY,
    WS_SUB_LIST_REDIS_KEY, WS_RATE_LIMIT_REDIS_KEY
)
from fetchers.config.queries import MUTUAL_BASE_QUOTE_QUERY
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.ratelimit import AsyncThrottler
from fetchers.utils.exceptions import (
    UnsuccessfulConnection, ConnectionClosed, InvalidStatusCode
)
from fetchers.helpers.ws import (
    make_sub_val,
    make_sub_redis_key,
    make_serve_redis_key
)


# Bitfinex only allows up to 30 subscriptions per ws connection
URI = "wss://api-pub.bitfinex.com/ws/2"
MAX_SUB_PER_CONN = 25
BACKOFF_MIN_SECS = 2.0
BACKOFF_MAX_SECS = 60.0

class BitfinexOHLCVWebsocket:
    '''
    Bitfinex OHLCV websocket fetcher
    '''

    def __init__(
        self, log_to_stream: bool = False, log_filename: str = None
    ):
        check_log_file = log_to_stream is False and log_filename is None
        if check_log_file:
            raise ValueError(
                "log_filename must be provided if not logging to stream"
            )

        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username=REDIS_USER,
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
        self.logger = create_logger(
            f'{EXCHANGE_NAME}_websocket',
            stream_handler=log_to_stream,
            log_filename=log_filename
        )

        # Rate limit manager
        # Limit to attempt to connect every 3 secs
        self.rate_limiter = AsyncThrottler(
            WS_RATE_LIMIT_REDIS_KEY.format(exchange = EXCHANGE_NAME),
            1,
            3,
            redis_client = self.redis_client
        )

        # Backoff
        self.backoff_delay = BACKOFF_MIN_SECS

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
                # async with self.rate_limiter:
                async with websockets.connect(URI, ping_interval=10) as ws:
                    await asyncio.gather(
                        *(self.subscribe_one(symbol, ws) for symbol in symbols))
                    self.logger.info(f"Connection {i}: Successful")
                    self.backoff_delay = BACKOFF_MIN_SECS
                    while True:
                        resp = await ws.recv()
                        respj = json.loads(resp)

                        # If resp is dict, find the symbol using wssymbol_mapping
                        #   and then map chanID to found symbol
                        # If resp is list, make sure its length is 6
                        #   and use the mappings to find symbol and push to Redis
                        if isinstance(respj, dict):
                            if 'event' in respj:
                                if respj['event'] == "subscribed":
                                    symbol = self.wssymbol_mapping[respj['key']]
                                    self.chanid_mapping[respj['chanId']] = symbol
                                elif respj['event'] == "error":
                                    self.logger.error(
                                        f"Connection {i}: Subscription failed, raising exception")
                                    raise UnsuccessfulConnection           
                        elif isinstance(respj, list):
                            if len(respj) == 2 and len(respj[1]) == 6:
                                try:
                                    symbol = self.chanid_mapping[respj[0]]
                                    timestamp = int(respj[1][0])
                                    open_ = respj[1][1]
                                    high_ = respj[1][3]
                                    low_ = respj[1][4]
                                    close_ = respj[1][2]
                                    volume_ = respj[1][5]
                                    base_id = self.rest_fetcher.symbol_data[symbol]['base_id']
                                    quote_id = self.rest_fetcher.symbol_data[symbol]['quote_id']

                                    # We make "sub'ed value" by serializing the price
                                    #   data so it can be put into Redis
                                    # Then, unique sub'ed and serve Redis keys
                                    #   are created to store the above sub'ed value
                                    # Also note here that the value put into the sub_
                                    #   and the serve_ keys are different:
                                    #   >> sub_ key is a hash with its [internal]
                                    #   keys as timestamps; this is so that the updater
                                    #   script can bulk-insert all the price data in
                                    #   the sub_ key to PSQL later, meanwhile...
                                    #   >> serve_ key is a hash with its [internal]
                                    #   keys as timestamp, o, h, l, c, v; this is
                                    #   because the web service (a.k.a. FastAPI) only
                                    #   serves to the user [via its websocket connection]
                                    #   the latest price data, meaning when any new data
                                    #   comes in from the exchange's ws API,
                                    #   the hash gets updated

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
                                    ws_serve_redis_key = make_serve_redis_key(
                                        EXCHANGE_NAME,
                                        base_id,
                                        quote_id,
                                        REDIS_DELIMITER
                                    )

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
                                    self.logger.warning(
                                        f"Bitfinex WS Fetcher: EXCEPTION: {exc}")

                        # Sleep to release event loop
                        await asyncio.sleep(0.01)
            except (ConnectionClosed, InvalidStatusCode) as exc:
                self.logger.warning(
                    f"Connection {i} raised exception: {exc} - reconnecting..."
                )
                await asyncio.sleep(min(self.backoff_delay, BACKOFF_MAX_SECS))
                self.backoff_delay *= (1+random.random()) # add a random factor

    async def mutual_basequote(self) -> None:
        '''
        Subscribes to all base-quote's that are available
        in all exchanges
        '''

        symbols_dict = self.rest_fetcher.get_symbols_from_exch(MUTUAL_BASE_QUOTE_QUERY)
        self.rest_fetcher.close_connections()
        await asyncio.gather(self.subscribe(symbols_dict.keys()))

    async def all(self) -> None:
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
        '''
        API to run the `mutual base-quote` method
        '''

        asyncio.run(self.mutual_basequote())

    def run_all(self) -> None:
        '''
        API to run the `all` method
        '''

        asyncio.run(self.all())
