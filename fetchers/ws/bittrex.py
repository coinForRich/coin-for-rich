#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Last tested 2020/09/24 on Python 3.8.5
#
# Note: This file is intended solely for testing purposes and may only be used
#   as an example to debug and compare with your code. The 3rd party libraries
#   used in this example may not be suitable for your production use cases.
#   You should always independently verify the security and suitability of any
#   3rd party library used in your code.

# https://github.com/slazarov/python-signalr-client
from signalr_aio import Connection
from base64 import b64decode
from zlib import decompress, MAX_WBITS
import hashlib
import hmac
import json
import asyncio
import time
import uuid
import redis
from common.config.constants import (
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_DELIMITER,
    DEFAULT_DATETIME_STR_RESULT
)
from common.helpers.datetimehelpers import str_to_milliseconds
from fetchers.config.constants import (
    WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY, WS_SUB_LIST_REDIS_KEY
)
from fetchers.rest.bittrex import BittrexOHLCVFetcher, EXCHANGE_NAME


URI = 'https://socket-v3.bittrex.com/signalr'
API_KEY = ''
API_SECRET = ''

HUB = None
LOCK = asyncio.Lock()
INVOCATION_EVENT = None
INVOCATION_RESPONSE = None

class BittrexOHLCVWebsocket:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )

        # SignalR hub & asyncio
        self.signalr_hub = None
        self.asyncio_lock = asyncio.Lock()
        self.invocation_event = None
        self.invocation_response = None

        # Rest fetcher for convenience
        self.rest_fetcher = BittrexOHLCVFetcher()

    async def connect(self):
        connection = Connection(URI)
        self.signalr_hub = connection.register_hub('c3')
        connection.received += self.on_message
        connection.error += self.on_error
        connection.start()
        print('Connected')

    async def authenticate(self):
        timestamp = str(int(time.time()) * 1000)
        random_content = str(uuid.uuid4())
        content = timestamp + random_content
        signed_content = hmac.new(
            API_SECRET.encode(), content.encode(), hashlib.sha512).hexdigest()

        response = await self.invoke(
            'Authenticate',
            API_KEY,
            timestamp,
            random_content,
            signed_content
        )

        if response['Success']:
            print('Authenticated')
            self.signalr_hub.client.on('authenticationExpiring', self.on_auth_expiring)
        else:
            print('Authentication failed: ' + response['ErrorCode'])

    async def subscribe(self, symbols):
        '''
        Subscribes to Bittrex WS for `symbols`

        :params:
            `symbols` list of symbols
                e.g., ['ETH-BTC', 'BTC-EUR']
        '''

        self.signalr_hub.client.on('heartbeat', self.on_heartbeat)
        # self.signalr_hub.client.on('trade', on_trade)
        self.signalr_hub.client.on('candle', self.on_candle)
        channels = [
            'heartbeat',
            # 'trade_BTC-USD',
            # 'candle_BTC-USD_MINUTE_1'
            *[f'candle_{symbol}_MINUTE_1' for symbol in symbols]
        ]

        response = await self.invoke('Subscribe', channels)
        for i in range(len(channels)):
            if response[i]['Success']:
                print('Subscription to "' + channels[i] + '" successful')
            else:
                print('Subscription to "' +
                    channels[i] + '" failed: ' + response[i]['ErrorCode'])

    async def invoke(self, method, *args):
        async with self.asyncio_lock:
            self.invocation_event = asyncio.Event()
            self.signalr_hub.server.invoke(method, *args)
            await self.invocation_event.wait()
            return self.invocation_response

    async def on_message(self, **msg):
        if 'R' in msg:
            self.invocation_response = msg['R']
            self.invocation_event.set()

    async def on_error(self, msg):
        print(msg)

    async def on_heartbeat(self, msg):
        print('\u2661')

    async def on_auth_expiring(self, msg):
        print('Authentication expiring...')
        asyncio.create_task(self.authenticate())

    async def on_trade(self, msg):
        await self.decode_message('Trade', msg)

    async def on_candle(self, msg):
        respj = await self.decode_message('Candle', msg)
        try:
            # If resp is dict, process and push to Redis
            # Convert timestamp to milliseconds first
            #   for conformity with the WS updater and other exchanges
            if isinstance(respj, dict):
                print(f'At time {round(time.time(), 2)}, response: {respj}')
                symbol = respj['marketSymbol']
                ohlcv = respj['delta']
                timestamp = str_to_milliseconds(
                    ohlcv['startsAt'], DEFAULT_DATETIME_STR_RESULT)
                open_ = ohlcv['open']
                high_ = ohlcv['high']
                low_ = ohlcv['low']
                close_ = ohlcv['close']
                volume_ = ohlcv['volume']
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
                # Replace ws serve key hash if this timestamp
                #   is more up-to-date
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
            print(f'{exc}')

    async def decode_message(self, title, msg):
        decoded_msg = await self.process_message(msg[0])
        return decoded_msg

    async def process_message(self, message):
        try:
            decompressed_msg = decompress(
                b64decode(message, validate=True), -MAX_WBITS)
        except SyntaxError:
            decompressed_msg = decompress(b64decode(message, validate=True))
        return json.loads(decompressed_msg.decode())

    async def main(self):
        '''
        Subscribes to some symbols
        '''
        
        await self.connect()
        if API_SECRET != '':
            await self.authenticate()
        else:
            print('Authentication skipped because API key was not provided')
        symbols = ["ETH-BTC", "BTC-EUR"]
        await self.subscribe(symbols)
        forever = asyncio.Event()
        await forever.wait()
    
    async def mutual_basequote(self):
        '''
        Subscribes to WS channels of the mutual symbols
            among all exchanges
        '''

        symbols_dict = self.rest_fetcher.get_mutual_basequote()
        self.rest_fetcher.close_connections()

        await self.connect()
        if API_SECRET != '':
            await self.authenticate()
        else:
            print('Authentication skipped because API key was not provided')
        await self.subscribe(symbols_dict.keys())
        forever = asyncio.Event()
        await forever.wait()

    def run_main(self):
        asyncio.run(self.main())

    def run_mutual_basequote(self):
        asyncio.run(self.mutual_basequote())

# if __name__ == "__main__":
#     asyncio.run(main())
