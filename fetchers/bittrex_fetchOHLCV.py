# This script fetches bittrex 1-minute OHLCV data

import asyncio
import time
import datetime
import psycopg2
import httpx
import backoff
from asyncio_throttle import Throttler
from fetchers.helpers.datetimehelpers import datetime_to_seconds
from fetchers.helpers.dbhelpers import psql_copy_from_csv
from fetchers.bitfinex_fetchOHLCV import EXCHANGE_NAME
from fetchers.config.constants import *


# Bittrex returns:
#   1-min or 5-min time windows in a period of 1 day
#   1-hour time windows in a period of 31 days
#   1-day time windows in a period of 366 days
EXCHANGE_NAME = "bittrex"
BASE_URL = "https://api.bittrex.com/v3"
OHLCV_INTERVALS = ["MINUTE_1", "MINUTE_5", "HOUR_1", "DAY_1"]
DAYDELTAS = {"MINUTE_1": 1, "MINUTE_5": 1, "HOUR_1": 31, "DAY_1": 366}
OHLCV_INTERVAL = "MINUTE_1"
RATE_LIMIT_HITS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN']['bittrex']
RATE_LIMIT_SECS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']


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

def onbackoff_ratelimit_handler(details):
    details['args'][2].rate_limit -= 1

def onsuccessgiveup_ratelimit_handler(details):
    details['args'][2].rate_limit = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN'][EXCHANGE_NAME]

@backoff.on_predicate(backoff.fibo, lambda result: result[0] == 429, max_tries=12, on_backoff=onbackoff_ratelimit_handler, on_success=onsuccessgiveup_ratelimit_handler, on_giveup=onsuccessgiveup_ratelimit_handler)
async def get_ohlcv_data(httpx_client, ohlcv_url, throttler):
    '''
    get ohlcv data based on url
    returns tuple:
        (
            http status (None if there's none),
            ohlcvs (None if there's none),
            exception type (None if there's none),
            error message (None if there's none)

        )
    params:
        `httpx_client`:
        `ohlcv_url`:
        `throtter`:
    '''

    try:
        ohlcvs_resp = await httpx_client.get(ohlcv_url)
        ohlcvs_resp.raise_for_status()
        return (
            ohlcvs_resp.status_code,
            ohlcvs_resp.json(),
            None,
            None
        )
    except httpx.HTTPStatusError as exc:
        resp_status_code = exc.response.status_code
        return (
            resp_status_code,
            None,
            type(exc),
            f'EXCEPTION: Response status code: {resp_status_code} while requesting {exc.request.url}'
        )
    except httpx.RequestError as exc:
        return (
            None,
            None,
            type(exc),
            f'EXCEPTION: Request error while requesting {exc.request.url}'
        )

def parse_ohlcvs(ohlcvs, symbol, base_id, quote_id):
    '''
    returns rows of parsed ohlcv
    params:
        `ohlcvs`: a list of ohlcv dicts (returned from request)
        `symbol`: string
        `base_id`: string
        `quote_id`: string
    '''

    ohlcvs_table_insert = []
    symexch_table_insert = []

    for ohlcv in ohlcvs:
        ohlcvs_table_insert.append(
            (   
                ohlcv['startsAt'],
                EXCHANGE_NAME,
                base_id,
                quote_id,
                ohlcv['open'],
                ohlcv['high'],
                ohlcv['low'],
                ohlcv['close'],
                ohlcv['volume']
            )
        )
        symexch_table_insert.append(
            (
                EXCHANGE_NAME,
                base_id,
                quote_id,
                symbol
            )
        )

    return (ohlcvs_table_insert, symexch_table_insert)

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
        `symbol_data`: dict (of symbol data)
        `symbol`: string
        `start_date`: datetime obj
        `interval`: string (for interval, e.g., "MINUTE_1")
        `httpx_client`: httpx Client object
        `psycopg2_conn`: connection object of psycopg2
        `psycopg2_cursor`: cursor object of psycopg2
        `fetch_start_time`: datetime obj - fetch starting time - the upper limit of the time range to fetch
        `throttler`: Throttler object from Throttler
    '''

    base_id = symbol_data['baseCurrencySymbol'].upper()
    quote_id = symbol_data['quoteCurrencySymbol'].upper()
    
    # Convert datetime with tzinfo to non-tzinfo
    start_date = start_date.replace(tzinfo=None)

    while start_date < fetch_start_time:
        delta = datetime.datetime.now() - start_date
        # Fetch historical data if time difference between now and start date is > 1 day
        if delta.days > 1:
            ohlcv_section = "historical"
        else:
            ohlcv_section = None
        ohlcv_url = make_ohlcv_url(BASE_URL, symbol, interval, ohlcv_section, start_date)
        
        async with throttler:
            result = await get_ohlcv_data(httpx_client, ohlcv_url, throttler)
            resp_status_code = result[0]
            ohlcvs = result[1]
            exc_type = result[2]
            exception_msg = result[3]
            # If ohlcvs is not an empty list or not None, process
            # Else, process the error and reduce rate limt
            if ohlcvs:
                try:
                    ohlcvs_parsed = parse_ohlcvs(ohlcvs, symbol, base_id, quote_id)
                    psql_copy_from_csv(psycopg2_conn, ohlcvs_parsed[0], "ohlcvs")
                    psql_copy_from_csv(psycopg2_conn, ohlcvs_parsed[1], "symbol_exchange")
                except Exception as exc:
                    exc_type = type(exc)
                    exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc} with original response as: {ohlcvs}'
                    error_tuple = make_error_tuple(fetch_start_time, symbol, start_date, interval, ohlcv_section, resp_status_code, exc_type, exception_msg)
                    psycopg2_cursor.execute(error_tuple[0], error_tuple[1])
                    print(exception_msg)
            else:
                error_tuple = make_error_tuple(fetch_start_time, symbol, start_date, interval, ohlcv_section, resp_status_code, exc_type, exception_msg)
                psycopg2_cursor.execute(error_tuple[0], error_tuple[1])
                print(exception_msg)
            
            # Increment start_date by the length according to `interval`
            psycopg2_conn.commit()
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
        conn = psycopg2.connect(DBCONNECTION)
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

async def bittrex_fetchOHLCV_symbol_OnDemand(symbol, start_date_dt, end_date_dt, client=None, conn=None, cur=None, throttler=None):
    '''
    Function to get OHLCVs of a symbol on demand
    params:
        `symbol`: string, uppercase
        `start_date_dt`: datetime obj (for start date)
        `end_date_dt`: datetime obj (for end date)
        `httpx_client`:
        `conn`: psycopg2 connection obj
        `cur`: psycopg2 cursor obj
        `throttler`: asyncio_throttler Throttler obj
    '''
    
    # Async httpx client
    self_httpx_c = False
    if not client:
        self_httpx_c = True
        limits = httpx.Limits(max_connections=HTTPX_MAX_CONCURRENT_CONNECTIONS)
        client = httpx.AsyncClient(timeout=None, limits=limits)

    # Postgres connection
    self_conn = False
    self_cur = False
    if not conn:
        self_conn = True
        conn = psycopg2.connect(DBCONNECTION)
    if not cur:
        self_cur = True  
        cur = conn.cursor()

    # Async throttler
    if not throttler:
        throttler = Throttler(
            rate_limit=RATE_LIMIT_HITS_PER_MIN, period=RATE_LIMIT_SECS_PER_MIN
        )
    loop = asyncio.get_event_loop()
    symbol_tasks = []

    # Load market data
    markets_url = f'{BASE_URL}/markets'
    markets_resp = await client.get(markets_url)
    market_data = markets_resp.json()

    # Lookup symbol dict of this symbol
    symbol_dict = None
    for sd in market_data:
        if sd['symbol'] == symbol:
            symbol_dict = sd

    print(f"=== Fetching OHLCVs for symbol {symbol}")
    symbol_tasks.append(loop.create_task(bittrex_fetchOHLCV_symbol(
        symbol_data=symbol_dict,
        symbol=symbol,
        start_date=start_date_dt,
        interval=OHLCV_INTERVAL,
        httpx_client=client,
        psycopg2_conn=conn,
        psycopg2_cursor=cur,
        fetch_start_time=end_date_dt,
        throttler=throttler
    )))
    await asyncio.wait(symbol_tasks)
    if self_cur:
        cur.close()
    if self_conn:
        conn.close()
    if self_httpx_c:
        await client.aclose()

def run():
    asyncio.run(bittrex_fetchOHLCV_all_symbols(datetime.datetime(2019, 9, 1)))

def run_OnDemand(symbol, start_date_dt, end_date_dt):
    asyncio.run(bittrex_fetchOHLCV_symbol_OnDemand(symbol, start_date_dt, end_date_dt))


if __name__ == "__main__":
    run()


# def parse_ohlcv(ohlcv, symbol, base_id, quote_id, fetch_start_time):
#     '''
#     returns parsed ohlcv in a tuple
#     params:
#         `ohlcv`: dict (ohlcv returned from request)
#         `symbol`: string
#         `base_id`: string
#         `quote_id`: string
#         `fetch_start_time`: datetime obj
#     '''

#     return (ohlcv['startsAt'],
#             EXCHANGE_NAME,
#             symbol,
#             base_id,
#             quote_id,
#             ohlcv['open'],
#             ohlcv['high'],
#             ohlcv['low'],
#             ohlcv['close'],
#             ohlcv['volume'],
#             fetch_start_time
#     )