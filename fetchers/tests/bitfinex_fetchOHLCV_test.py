### This script fetches bitfinex 1-minute OHLCV data

import asyncio
import time
import datetime
import psycopg2
import httpx
from tqdm import tqdm
import fetchers.helpers.datetimehelpers


PAIR_EXCHANGE_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"
LIST_CURRENCY_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:currency"


async def bitfinex_load_marketdata():
    '''
    loads market data into a dict of this form:
        {
            '1INCH:USD': {
                'base_id': "1INCH",
                'quote_id': "USD"
            },
            'some_other_symbol': {
                'base_id': "ABC",
                'quote_id': "XYZ"
            }
            ...
        }
    '''

    market_data = {}
    async with httpx.AsyncClient() as client:
        pair_ex_resp = await client.get(PAIR_EXCHANGE_URL)
        list_cur_resp = await client.get(LIST_CURRENCY_URL)
        pair_ex = pair_ex_resp.json()[0]
        list_cur = list_cur_resp.json()[0]
        start = time.time()
        for symbol in pair_ex:
            # e.g., 1INCH:USD
            market_data[symbol] = {}
            for currency in list_cur:
                if "" in symbol.split(currency):
                    if symbol.split(currency).index("") == 0:
                        market_data[symbol]['base_id'] = currency
                    else:
                        market_data[symbol]['quote_id'] = currency
        end = time.time()
        d = end - start
        print(market_data)
        print(d)
        return market_data

def run_test():
    asyncio.run(bitfinex_load_marketdata())


if __name__ == "__main__":
    run_test()