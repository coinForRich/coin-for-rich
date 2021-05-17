### This script fetches bittrex 1-minute OHLCV data

import asyncio
import time
import datetime
import psycopg2
import httpx
from collections.abc import Mapping


# Bittrex returns:
#   1-min or 5-min time windows in a period of 1 day
#   1-hour time windows in a period of 31 days
#   1-day time windows in a period of 366 days
BASE_URL = "https://api.bittrex.com/v3"
INTERVALS = ["MINUTE_1", "MINUTE_5", "HOUR_1", "DAY_1"]
DAYDELTAS = {"MINUTE_1": 1, "MINUTE_5": 1, "HOUR_1": 31, "DAY_1": 366}
RATE_LIMIT_SECS = 1.5


def make_ohlcv_url(base_url, symbol, interval, historical, start_date):
    '''
    returns OHLCV url: string
    params:
        `base_url`: string - base URL (see BASE_URL)
        `symbol`: string - symbol
        `interval`: string - interval type (see INTERVALS)
        `start_date`: datetime.datetime object
    '''
    if historical:
        if interval == "MINUTE_1" or interval == "MINUTE_5":
            return f'{base_url}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}/{start_date.day}'
        elif interval == "HOUR_1":
            return f'{base_url}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}'
        elif interval == "DAY_1":
            return f'{base_url}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}'
    return f'{base_url}/markets/{symbol}/candles/{interval}/recent'


def parse_ohlcv(ohlcv, baseId, quoteId):
    '''
    returns parsed ohlcv: list
    params:
        `ohlcv`: dict (ohlcv returned from request)
    '''
    try:
        return [ ohlcv['startsAt'], ohlcv['open'], ohlcv['high'], ohlcv['low'], ohlcv['close'], ohlcv['volume'], baseId, quoteId, datetime.datetime.now() ]
    except:
        return [ None, None, None, None, None, None, None, None, datetime.datetime.now() ]

async def bittrex_fetchOHLCV_test():
    async with httpx.AsyncClient(timeout=None) as client:
        CONNECTION = "dbname=postgres user=postgres password=horus123 host=localhost port=15432"
        conn = psycopg2.connect(CONNECTION)
        cur = conn.cursor()

        markets_url = f'{BASE_URL}/markets'
        markets_resp = await client.get(markets_url)
        markets = markets_resp.json()

        for market in markets:
            symbol = market['symbol']
            baseId = market['baseCurrencySymbol'].lower()
            quoteId = market['quoteCurrencySymbol'].lower()
            start_date = datetime.datetime(2020, 11, 1)
            for interval in INTERVALS:
                # if time difference is within 1 day, fetch recent data
                while int(start_date.timestamp()) < int(time.time()):
                    if (int(time.time()) - int(start_date.timestamp())) > 86400:
                        ohlcv_url = make_ohlcv_url(BASE_URL, symbol, interval, "historical", start_date)
                    else:
                        ohlcv_url = make_ohlcv_url(BASE_URL, symbol, interval, None, start_date)
                    time.sleep(RATE_LIMIT_SECS)
                    print(f'=== Getting url {ohlcv_url}')
                    ohlcvs_resp = await client.get(ohlcv_url)
                    if ohlcvs_resp.status_code == 200:
                        ohlcvs = ohlcvs_resp.json()
                        try:
                            start_date += datetime.timedelta(days=DAYDELTAS[interval])
                            for ohlcv in ohlcvs:
                                ohlcv = parse_ohlcv(ohlcv, baseId, quoteId)
                                print(f'=== Inserting into bittrex_ohlcv values {ohlcv}')
                                cur.execute("INSERT INTO bittrex_ohlcv VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);", ohlcv)
                            conn.commit()
                        except Exception as e:
                            print(f'=== Error in {ohlcvs} with exception {e}')
                            break
                    else:
                        print(f'=== Error in url: {ohlcv_url} - status code {ohlcvs_resp.status_code}')
                        break
        conn.close()

def run_test():
    asyncio.run(bittrex_fetchOHLCV_test())


if __name__ == "__main__":
    run_test()