# Adapted from Bittrex Python WS client example
#
# Last tested 2020/09/24 on Python 3.8.5
# Note: This file is intended solely for testing purposes and may only be used
#   as an example to debug and compare with your code. The 3rd party libraries
#   used in this example may not be suitable for your production use cases.
#   You should always independently verify the security and suitability of any
#   3rd party library used in your code.

# https://github.com/slazarov/python-signalr-client


import hashlib
import hmac
import json
import logging
import asyncio
import random
import uuid
import redis
from typing import Any, Iterable, NoReturn, Union
from signalr_aio import Connection
from base64 import b64decode
from zlib import decompress, MAX_WBITS
from common.config.constants import (
    REDIS_HOST, REDIS_USER,
    REDIS_PASSWORD, REDIS_DELIMITER,
    DEFAULT_DATETIME_STR_RESULT
)
from common.utils.logutils import create_logger
from common.helpers.datetimehelpers import str_to_milliseconds, redis_time
from fetchers.config.constants import (
    WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY, WS_SUB_LIST_REDIS_KEY
)
from fetchers.config.queries import MUTUAL_BASE_QUOTE_QUERY
from fetchers.rest.bittrex import BittrexOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.exceptions import (
    ConnectionClosed, UnsuccessfulConnection, InvalidStatusCode
)
from fetchers.helpers.ws import (
    make_sub_val,
    make_sub_redis_key,
    make_serve_redis_key
)


URI = 'https://socket-v3.bittrex.com/signalr'
API_KEY = ''
API_SECRET = ''
BACKOFF_MIN_SECS = 2.0
BACKOFF_MAX_SECS = 60.0

class BittrexOHLCVWebsocket:
    '''
    Bittrex OHLCV websocket fetcher
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

        # SignalR hub & asyncio
        self.signalr_hub = None
        self.asyncio_lock = asyncio.Lock()
        self.invocation_event = None
        self.invocation_response = None
        self.subscription_success = False

        # Rest fetcher for convenience
        self.rest_fetcher = BittrexOHLCVFetcher()

        # Latest timestamp with data
        self.latest_ts = None

        # Logging
        self.logger = create_logger(
            f'{EXCHANGE_NAME}_websocket',
            stream_handler=log_to_stream,
            log_filename=log_filename
        )

        # Backoff
        self.backoff_delay = BACKOFF_MIN_SECS

    async def _connect(self) -> None:
        self.latest_ts = redis_time(self.redis_client)
        connection = Connection(URI)
        self.signalr_hub = connection.register_hub('c3')
        connection.received += self.on_message
        connection.error += self.on_error
        connection.start()
        self.logger.info('Connected')

    async def _authenticate(self) -> None:
        timestamp = str(int(redis_time(self.redis_client)) * 1000)
        random_content = str(uuid.uuid4())
        content = timestamp + random_content
        signed_content = hmac.new(
            API_SECRET.encode(), content.encode(), hashlib.sha512).hexdigest()

        response = await self._invoke(
            'Authenticate',
            API_KEY,
            timestamp,
            random_content,
            signed_content
        )

        if response['Success']:
            self.logger.info('Authenticated')
            self.signalr_hub.client.on('authenticationExpiring', self.on_auth_expiring)
        else:
            self.logger.warning('Authentication failed: ' + response['ErrorCode'])

    async def _subscribe(self, symbols: Iterable, i: int = 0) -> None:
        '''
        Subscribes to Bittrex WS for `symbols`

        :params:
            `symbols` list of symbols
                e.g., ['ETH-BTC', 'BTC-EUR']
        '''

        self.subscription_success = False

        # self.signalr_hub.client.on('trade', on_trade)
        self.signalr_hub.client.on('heartbeat', self.on_heartbeat)
        self.signalr_hub.client.on('candle', self.on_candle)
        channels = (
            'heartbeat',
            # 'candle_BTC-USD_MINUTE_1'
            *(f'candle_{symbol}_MINUTE_1' for symbol in symbols)
        )

        response = await self._invoke('Subscribe', channels)
        for c in range(len(channels)):
            if response[c]['Success']:
                # Only one success is enough to switch to True
                self.subscription_success = True
            else:
                self.logger.error(
                    f"Group {i}: Subscription to {channels[c]} failed: {response[c]['ErrorCode']}")
                # raise UnsuccessfulConnection // not a good idea to raise here
        if self.subscription_success:
            self.logger.info(f"Group {i}: Subscription successful")

    async def _invoke(self, method: str, *args) -> Union[Any, None]:
        '''
        Invokes a method

        Default function from template
        '''

        async with self.asyncio_lock:
            self.invocation_event = asyncio.Event()
            self.signalr_hub.server.invoke(method, *args)
            await self.invocation_event.wait()
            return self.invocation_response

    async def on_message(self, **msg) -> None:
        '''
        Action to take on message

        Default function from template
        '''

        if 'R' in msg:
            self.invocation_response = msg['R']
            self.invocation_event.set()

    async def on_error(self, msg) -> None:
        '''
        Action to take on error

        Default function from template
        '''

        self.latest_ts = redis_time(self.redis_client)
        self.logger.warning(msg)

    async def on_heartbeat(self, msg) -> None:
        '''
        Action to take on heartbeat from websocket server

        Default function from template
        '''

        self.latest_ts = redis_time(self.redis_client)
        # self.logger.info('\u2661')

    async def on_auth_expiring(self, msg) -> None:
        '''
        Action to take on AUTH expiring

        Default function from template
        '''

        self.logger.info('Authentication expiring...')
        asyncio.create_task(self._authenticate())

    async def on_trade(self, msg) -> None:
        '''
        Action to take on trade response

        Default function from template
        '''

        self.latest_ts = redis_time(self.redis_client)
        await self.decode_message('Trade', msg)

    async def on_candle(self, msg) -> None:
        '''
        Action to take on candle response

        Default function from template
        '''

        self.latest_ts = redis_time(self.redis_client)
        respj = await self.decode_message('Candle', msg)

        # If resp is dict, process and push to Redis
        # Convert timestamp to milliseconds first
        #   for conformity with the WS updater and other exchanges
        if isinstance(respj, dict):
            try:
                symbol = respj['marketSymbol']
                ohlcv = respj['delta']
                timestamp = str_to_milliseconds(
                    ohlcv['startsAt'], DEFAULT_DATETIME_STR_RESULT)
                open_ = ohlcv['open']
                high_ = ohlcv['high']
                low_ = ohlcv['low']
                close_ = ohlcv['close']
                volume_ = ohlcv['volume']
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
                    WS_SUB_LIST_REDIS_KEY, ws_sub_redis_key
                )
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
                    f"Bittrex WS Fethcer: EXCEPTION: {exc}")

    async def decode_message(self, title, msg) -> None:
        '''
        Decodes message

        Default function from template
        '''

        decoded_msg = await self.process_message(msg[0])
        return decoded_msg

    async def process_message(self, message) -> None:
        '''
        Processes message

        Default function from template
        '''

        try:
            decompressed_msg = decompress(
                b64decode(message, validate=True), -MAX_WBITS)
        except SyntaxError:
            decompressed_msg = decompress(b64decode(message, validate=True))
        return json.loads(decompressed_msg.decode())

    async def subscribe(self, symbols: Iterable) -> NoReturn:
        '''
        Subscribes to WS channels of `symbols`
        '''

        while True:
            try:
                now = redis_time(self.redis_client)
                if self.signalr_hub is None \
                    or (now - self.latest_ts) > 60 \
                    or (not self.subscription_success):
                    await self._connect()
                    if API_SECRET != '':
                        await self._authenticate()
                    else:
                        self.logger.info('Authentication skipped because API key was not provided')
                    await self._subscribe(symbols)
            # Not sure what kind of exception we will encounter
            except (ConnectionClosed, InvalidStatusCode) as exc:
                self.logger.warning(
                    f"Connection raised exception: {exc} - reconnecting..."
                )
                await asyncio.sleep(min(self.backoff_delay, BACKOFF_MAX_SECS))
                self.backoff_delay *= (1+random.random()) # add a random factor

            # Sleep to release event loop
            await asyncio.sleep(0.01)

    async def all(self) -> NoReturn:
        '''
        Subscribes to WS channels of all symbols
        '''

        self.rest_fetcher.fetch_symbol_data()
        symbols =  tuple(self.rest_fetcher.symbol_data.keys())
        await asyncio.gather(self.subscribe(symbols))

    async def mutual_basequote(self) -> NoReturn:
        '''
        Subscribes to all base-quote's that are available
        in all exchanges
        '''

        symbols_dict = self.rest_fetcher.get_symbols_from_exch(MUTUAL_BASE_QUOTE_QUERY)
        await asyncio.gather(self.subscribe(symbols_dict.keys()))

    def run_mutual_basequote(self) -> None:
        '''
        API to run the `mutual base-quote` method
        '''

        # loop = asyncio.get_event_loop()
        # if loop.is_closed():
        #     asyncio.set_event_loop(asyncio.new_event_loop())
        #     loop = asyncio.get_event_loop()
        # try:
        #     loop.create_task(self.mutual_basequote())
        #     loop.run_forever()
        # finally:
        #     loop.close()
        asyncio.run(self.mutual_basequote())

    def run_all(self) -> None:
        '''
        API to run the `all` method
        '''

        asyncio.run(self.all())
