### This module uses websocket to fetch bitfinex 1-minute OHLCV data in real time

import sys
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
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.exceptions import UnsuccessfulConnection, ConnectionClosedOK


# Bitfinex only allows up to 30 subscriptions per ws connection

URI = "wss://api-pub.bitfinex.com/ws/2"

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

    async def subscribe_one(self, symbol, ws_client):
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

    async def subscribe(self, symbols):
        '''
        Subscribes to Bitfinex WS for `symbols`
        :params:
            `symbols` list of symbols
                e.g., ['ETHBTC', 'BTCEUR']
        '''

        while True:
            try:
                async with websockets.connect(URI) as ws:
                    await asyncio.gather(
                        *(self.subscribe_one(symbol, ws) for symbol in symbols))
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
                                    print(f'At time {round(time.time(), 2)}, response: {respj}')
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
            except ConnectionClosedOK:
                pass
            except Exception as exc:
                print(f"EXCEPTION: {exc}")

    async def mutual_basequote(self):
        '''
        Subscribes to WS channels of the mutual symbols
            among all exchanges
        '''

        symbols_dict = self.rest_fetcher.get_mutual_basequote()
        self.rest_fetcher.close_connections()
        # symbols = ["ETHBTC", "BTCEUR"]
        await asyncio.gather(self.subscribe(symbols_dict.keys()))
    
    def run_mutual_basequote(self):
        asyncio.run(self.mutual_basequote())


if __name__ == "__main__":
    run_cmd = sys.argv[1]
    ws_bitfinex = BitfinexOHLCVWebsocket()
    if getattr(ws_bitfinex, run_cmd):
        getattr(ws_bitfinex, run_cmd)()
