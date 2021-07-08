### This script uses websocket to fetch bitfinex 1-minute OHLCV data in real time

import asyncio
import json
import redis
import websockets
from common.config.constants import *
from fetchers.config.constants import WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher, EXCHANGE_NAME
from fetchers.utils.exceptions import UnsuccessfulConnection, ConnectionClosedOK


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

    async def connect(self, symbol, ws_client):
        '''
        Connects to WS endpoint for a symbol
        :params:
            `symbol`: string of symbol
            `ws_client`: websockets client obj
        '''

        tsymbol = BitfinexOHLCVFetcher.make_tsymbol(symbol)
        ws_symbol = f"trade:1m:{tsymbol}"
        self.wssymbol_mapping[ws_symbol] = symbol
        msg = {'event': 'subscribe',  'channel': 'candles', 'key': ws_symbol}
        await ws_client.send(json.dumps(msg))

    async def subscribe(self, symbols):
        '''
        Subscribes to Bitfinex WS for `symbols`
        :params:
            `symbols`: list of symbols (not tsymbol)
                e.g., [BTCUSD, ETHUSD]
        '''

        while True:
            try:
                async with websockets.connect(URI) as ws:
                    await asyncio.gather(*(self.connect(symbol, ws) for symbol in symbols))
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
                                    print(f'Response: {respj}')
                                    symbol = self.chanid_mapping[respj[0]]
                                    timestamp = respj[1][0]
                                    open = respj[1][1]
                                    high = respj[1][3]
                                    low = respj[1][4]
                                    close = respj[1][2]
                                    volume = respj[1][5]
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
                            print(f'{exc}')
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
