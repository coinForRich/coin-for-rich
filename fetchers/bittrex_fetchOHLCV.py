### This script fetches bittrex 1-minute OHLCV data

import asyncio
from fetchers.bitfinex_fetchOHLCV import EXCHANGE_NAME
import time
import datetime
import psycopg2
import httpx
from collections.abc import Mapping
from asyncio_throttle import Throttler
from fetchers.helpers.datetimehelpers import datetime_to_seconds


# Bittrex returns:
#   1-min or 5-min time windows in a period of 1 day
#   1-hour time windows in a period of 31 days
#   1-day time windows in a period of 366 days
EXCHANGE_NAME = "bittrex"
BASE_URL = "https://api.bittrex.com/v3"
OHLCV_INTERVALS = ["MINUTE_1", "MINUTE_5", "HOUR_1", "DAY_1"]
DAYDELTAS = {"MINUTE_1": 1, "MINUTE_5": 1, "HOUR_1": 31, "DAY_1": 366}
OHLCV_INTERVAL = "MINUTE_1"
RATE_LIMIT_HITS_PER_MIN = 45
RATE_LIMIT_SECS_PER_MIN = 60


def make_ohlcv_url(base_url, symbol, interval, historical, start_date):
    '''
    returns OHLCV url: string
    params:
        `base_url`: string - base URL (see BASE_URL)
        `symbol`: string - symbol
        `interval`: string - interval type (see INTERVALS)
        `start_date`: datetime object
    '''
    if historical:
        if interval == "MINUTE_1" or interval == "MINUTE_5":
            return f'{base_url}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}/{start_date.day}'
        elif interval == "HOUR_1":
            return f'{base_url}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}'
        elif interval == "DAY_1":
            return f'{base_url}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}'
    return f'{base_url}/markets/{symbol}/candles/{interval}/recent'


def parse_ohlcv(ohlcv, symbol, base_id, quote_id, fetch_start_time):
    '''
    returns parsed ohlcv in a tuple
    params:
        `ohlcv`: dict (ohlcv returned from request)
        `symbol`:
        `base_id`:
        `quote_id`:
        `fetch_start_time`:
        ...
    '''

    return (ohlcv['startsAt'],
            EXCHANGE_NAME,
            symbol,
            base_id,
            quote_id,
            ohlcv['open'],
            ohlcv['high'],
            ohlcv['low'],
            ohlcv['close'],
            ohlcv['volume'],
            fetch_start_time
    )

def make_error_tuple(fetch_start_time, symbol, start_date, interval, ohlcv_section, resp_status_code, exception_class, exception_msg):
    '''
    returns an error tuple to put into errors db
    params:
        `fetch_start_time`: datetime obj of fetch starting time
        `symbol`: string
        `start_date`: datetime obj of start date
        `time_frame`: string
        `ohlcv_section`: string
        `resp_status_code`: int - response status code
        `exception_class`: string
        `exception_msg`: string
    '''
    
    return ("INSERT INTO ohlcvs_errors VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
            (fetch_start_time,
            EXCHANGE_NAME,
            symbol,
            start_date,
            interval,
            ohlcv_section,
            resp_status_code,
            str(exception_class),
            exception_msg
    ))

async def bittrex_fetchOHLCV_symbol(symbol_data, symbol, start_date, interval, httpx_client, psycopg2_conn, psycopg2_cursor, fetch_start_time, throttler):
    '''
    custom function that fetches OHLCV for a symbol from start_date
    params:
        `market_data`: dict (of symbol data)
        `symbol`: string
        `start_date`: datetime obj
        `interval`: string (for interval, e.g., "MINUTE_1")
        `httpx_client`: httpx Client object
        `psycopg2_conn`: connection object of psycopg2
        `psycopg2_cursor`: cursor object of psycopg2
        `fetch_start_time`: datetime obj - fetch starting time
        `throttler`: Throttler object from Throttler
    '''

    base_id = symbol_data['baseCurrencySymbol'].upper()
    quote_id = symbol_data['quoteCurrencySymbol'].upper()

    while datetime_to_seconds(start_date) < datetime_to_seconds(fetch_start_time):
        # Fetch recent data if time difference is within 1 day
        if (datetime_to_seconds(fetch_start_time) - datetime_to_seconds(start_date)) > 86400:
            ohlcv_section = "historical"
        else:
            ohlcv_section = None
        ohlcv_url = make_ohlcv_url(BASE_URL, symbol, interval, ohlcv_section, start_date)
        
        async with throttler:
            try:
                ohlcvs_resp = await httpx_client.get(ohlcv_url)
                ohlcvs_resp.raise_for_status()
                ohlcvs = ohlcvs_resp.json()
                try:
                    for ohlcv in ohlcvs:
                        ohlcv = parse_ohlcv(ohlcv, symbol, base_id, quote_id, fetch_start_time)
                        psycopg2_cursor.execute(
                            "INSERT INTO ohlcvs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (time, exchange, symbol) DO NOTHING;;", ohlcv
                        )
                    psycopg2_conn.commit()
                except Exception as exc:
                    exception_msg = f'Error while processing ohlcv response: {exc}'
                    error_tuple = make_error_tuple(fetch_start_time, symbol, start_date, interval, ohlcv_section, ohlcvs_resp.status_code, type(exc), exception_msg)
                    psycopg2_cursor.execute(
                        error_tuple[0], error_tuple[1]
                    )
                    pass
            except httpx.HTTPStatusError as exc:
                resp_status_code = exc.response.status_code
                exception_msg = f'Error response {resp_status_code} while requesting {exc.request.url}'
                error_tuple = make_error_tuple(fetch_start_time, symbol, start_date, interval, ohlcv_section, resp_status_code, type(exc), exception_msg)
                psycopg2_cursor.execute(
                        error_tuple[0], error_tuple[1]
                )
                if resp_status_code == 429:
                    time.sleep(RATE_LIMIT_SECS_PER_MIN)
                pass
            except httpx.RequestError as exc:
                resp_status_code = None
                exception_msg = f'An error occurred while requesting {exc.request.url}'
                error_tuple = make_error_tuple(fetch_start_time, symbol, start_date, interval, ohlcv_section, resp_status_code, type(exc), exception_msg)
                psycopg2_cursor.execute(
                        error_tuple[0], error_tuple[1]
                )
                pass
            # Increment start_date by the length according to `interval`
            start_date += datetime.timedelta(days=DAYDELTAS[interval])

async def bittrex_fetchOHLCV_all_symbols(start_date_dt):
    '''
    Main function to fetch OHLCV for all trading symbols on Bittrex
    params:
        `start_date_dt`: datetime object (for starting date)
    '''

    limits = httpx.Limits(max_connections=50)
    async with httpx.AsyncClient(timeout=None, limits=limits) as client:
        # Postgres connection
        CONNECTION = "dbname=postgres user=postgres password=horus123 host=localhost port=15432"
        conn = psycopg2.connect(CONNECTION)
        cur = conn.cursor()

        # Async throttler / semaphore
        throttler = Throttler(rate_limit=RATE_LIMIT_HITS_PER_MIN, period=RATE_LIMIT_SECS_PER_MIN)
        loop = asyncio.get_event_loop()
        symbol_tasks = []

        # Load market data
        markets_url = f'{BASE_URL}/markets'
        markets_resp = await client.get(markets_url)
        market_data = markets_resp.json()
        fetch_start_time = datetime.datetime.now()

        for symbol_dict in market_data:
            print(f"=== Fetching OHLCVs for symbol {symbol_dict['symbol']}")
            symbol_tasks.append(loop.create_task(bittrex_fetchOHLCV_symbol(
                symbol_data=symbol_dict,
                symbol=symbol_dict['symbol'],
                start_date=start_date_dt,
                interval=OHLCV_INTERVAL,
                httpx_client=client,
                psycopg2_conn=conn,
                psycopg2_cursor=cur,
                fetch_start_time=fetch_start_time,
                throttler=throttler
            )))
        await asyncio.wait(symbol_tasks)
        conn.close()

def run():
    asyncio.run(bittrex_fetchOHLCV_all_symbols(datetime.datetime(2019, 1, 1)))


if __name__ == "__main__":
    run()