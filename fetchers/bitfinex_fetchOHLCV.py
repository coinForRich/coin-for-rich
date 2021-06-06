### This script fetches bitfinex 1-minute OHLCV data

import asyncio
import time
import datetime
import psycopg2
import httpx
import backoff
from asyncio_throttle import Throttler
from fetchers.helpers.datetimehelpers import *
from fetchers.helpers.dbhelpers import psql_copy_from_csv
from fetchers.config.constants import *


EXCHANGE_NAME = "bitfinex"
PAIR_EXCHANGE_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"
LIST_CURRENCY_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:currency"
OHLCV_TIMEFRAME = "1m"
OHLCV_SECTION_HIST = "hist"
OHLCV_SECTION_LAST = "last"
OHLCV_LIMIT = 9000
RATE_LIMIT_HITS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN']['bitfinex']
RATE_LIMIT_SECS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']


def make_tsymbol(symbol):
    '''
    returns appropriate trade symbol for bitfinex
    params:
        `symbol`: string (trading symbol, e.g., BTSE:USD)
    '''
    
    return f't{symbol}'

def make_ohlcv_url(time_frame, symbol, section, limit, start_date_mls, sort):
    '''
    returns OHLCV url: string
    params:
        `ohlcv_base_url`: 
        `time_frame`: string (time frame, e.g., 1m)
        `symbol`: string (trading symbol, e.g., BTSE:USD)
        `section`: string (whether is historical data or latest data)
        `limit`: int (number limit of results fetched)
        `start_date_mls`: int (datetime obj converted into milliseconds)
        `sort`: int (1 or -1)

    example: https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTSE:USD/hist?limit=10000&start=1577836800000&sort=1
    '''
    
    symbol = make_tsymbol(symbol)
    return "https://api-pub.bitfinex.com/v2/candles/trade:{time_frame}:{symbol}/{section}?limit={limit}&start={start_date_mls}&sort={sort}".format(time_frame=time_frame, symbol=symbol, section=section, limit=limit, start_date_mls=start_date_mls, sort=sort)

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

def parse_ohlcvs(ohlcvs, symbol, base_id, quote_id, ohlcv_section):
    '''
    returns rows of parsed ohlcv
    note:
        if ohlcv_section is `hist`, ohlcvs will be list of lists
        if ohlcv_section is `last`, ohlcvs will be a list
    params:
        `ohlcvs`: a list of ohlcv dicts (returned from request)
        `symbol`: string
        `base_id`: string
        `quote_id`: string
        `ohlcv_section`: string
    '''

    ohlcvs_table_insert = []
    symexch_table_insert = []

    if ohlcv_section == OHLCV_SECTION_HIST:
        for ohlcv in ohlcvs:
            ohlcvs_table_insert.append(
                (
                    milliseconds_to_datetime(ohlcv[0]),
                    EXCHANGE_NAME,
                    base_id,
                    quote_id,
                    ohlcv[1],
                    ohlcv[3],
                    ohlcv[4],
                    ohlcv[2],
                    ohlcv[5]
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
    else:
        ohlcvs_table_insert.append(
            (
                    milliseconds_to_datetime(ohlcvs[0]),
                    EXCHANGE_NAME,
                    base_id,
                    quote_id,
                    ohlcvs[1],
                    ohlcvs[3],
                    ohlcvs[4],
                    ohlcvs[2],
                    ohlcvs[5]
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

def make_error_tuple(fetch_start_time, symbol, start_date, time_frame, ohlcv_section, resp_status_code, exception_class, exception_msg):
    '''
    returns:
        a SQL insert string to error table
        a tuple to insert into the error table
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
    # TODO: check instance of start_date whether it's datetime or int
    # and convert accordingly

    return ("INSERT INTO ohlcvs_errors VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
            (fetch_start_time,
            EXCHANGE_NAME,
            symbol,
            start_date,
            time_frame,
            ohlcv_section,
            resp_status_code,
            str(exception_class),
            exception_msg
    ))

async def bitfinex_fetchOHLCV_symbol(symbol_data, symbol, start_date, time_frame, limit, sort, httpx_client, psycopg2_conn, psycopg2_cursor, fetch_start_time, throttler):
    '''
    custom function that fetches OHLCV for a symbol from start_date
    params:
        `symbol_data`: dict (of symbol data)
        `symbol`: string
        `start_date`: datetime obj
        `time_frame`: string
        `limit`: int
        `httpx_client`: httpx Client object
        `psycopg2_conn`: connection object of psycopg2
        `psycopg2_cursor`: cursor object of psycopg2
        `fetch_start_time`: datetime obj - fetch starting time - the upper limit of the time range to fetch
        `throttler`: Throttler object from Throttler
    '''
    
    try:
        base_id = symbol_data[symbol]['base_id']
        quote_id = symbol_data[symbol]['quote_id']
    except:
        print(symbol_data)

    # Convert datetime with tzinfo to non-tzinfo
    start_date = start_date.replace(tzinfo=None)

    start_date_mls = datetime_to_milliseconds(start_date)
    fetch_start_time_mls = datetime_to_milliseconds(fetch_start_time)

    while start_date_mls < fetch_start_time_mls:
        # Fetch historical data if time difference between now and start date is > 60 secs
        if datetime_to_milliseconds(datetime.datetime.now()) - start_date_mls > 60000:
            ohlcv_section = OHLCV_SECTION_HIST
        else:
            ohlcv_section = OHLCV_SECTION_LAST
        ohlcv_url = make_ohlcv_url(time_frame, symbol, ohlcv_section, limit, start_date_mls, sort)

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
                    ohlcvs_parsed = parse_ohlcvs(ohlcvs, symbol, base_id, quote_id, ohlcv_section)
                    psql_copy_from_csv(psycopg2_conn, ohlcvs_parsed[0], "ohlcvs")
                    psql_copy_from_csv(psycopg2_conn, ohlcvs_parsed[1], "symbol_exchange")
                    if ohlcv_section == OHLCV_SECTION_HIST:
                        ohlcvs_last_date = ohlcvs[-1][0]
                    else:
                        ohlcvs_last_date = ohlcvs[0]
                    if ohlcvs_last_date > start_date_mls:
                        start_date_mls = ohlcvs_last_date
                    else:
                        start_date_mls += 60000
                except Exception as exc:
                    resp_status_code = result[0]
                    exc_type = type(exc)
                    exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc} with original response as: {ohlcvs}'
                    error_tuple = make_error_tuple(fetch_start_time, symbol, start_date, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg)
                    psycopg2_cursor.execute(error_tuple[0], error_tuple[1])
                    print(exception_msg)
                    start_date_mls += 60000
            else:
                error_tuple = make_error_tuple(fetch_start_time, symbol, start_date, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg)
                psycopg2_cursor.execute(error_tuple[0], error_tuple[1])
                print(exception_msg)
                start_date_mls += 60000
            psycopg2_conn.commit()

def bitfinex_load_symboldata():
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

    symbol_data = {}
    with httpx.Client() as client:
        pair_ex_resp = client.get(PAIR_EXCHANGE_URL)
        list_cur_resp = client.get(LIST_CURRENCY_URL)
        pair_ex = pair_ex_resp.json()[0]
        list_cur = list_cur_resp.json()[0]
        for symbol in pair_ex:
            # e.g., 1INCH:USD
            symbol_data[symbol] = {}
            for currency in list_cur:
                if "" in symbol.split(currency):
                    if symbol.split(currency).index("") == 0:
                        symbol_data[symbol]['base_id'] = currency
                    else:
                        symbol_data[symbol]['quote_id'] = currency
        return symbol_data

async def bitfinex_fetchOHLCV_all_symbols(start_date_dt):
    '''
    Main function to fetch OHLCV for all trading symbols on Bitfinex
    params:
        `start_date_dt`: datetime object (for starting date)
    '''

    limits = httpx.Limits(max_connections=32)
    async with httpx.AsyncClient(timeout=None, limits=limits) as client:
        # Postgres connection
        conn = psycopg2.connect(DBCONNECTION)
        cur = conn.cursor()

        # Async throttler
        throttler = Throttler(rate_limit=RATE_LIMIT_HITS_PER_MIN, period=RATE_LIMIT_SECS_PER_MIN)
        loop = asyncio.get_event_loop()
        symbol_tasks = []

        # Load symbol data
        symbol_data = bitfinex_load_symboldata()
        fetch_start_time = datetime.datetime.now()

        for symbol in symbol_data.keys():
            print(f'=== Fetching OHLCVs for symbol {symbol}')
            symbol_tasks.append(loop.create_task(bitfinex_fetchOHLCV_symbol(
                symbol_data=symbol_data,
                symbol=symbol,
                start_date=start_date_dt,
                time_frame=OHLCV_TIMEFRAME,
                limit=OHLCV_LIMIT,
                sort=1,
                httpx_client=client,
                psycopg2_conn=conn,
                psycopg2_cursor=cur,
                fetch_start_time=fetch_start_time,
                throttler=throttler
            )))
        await asyncio.wait(symbol_tasks)
        conn.close()

async def bitfinex_fetchOHLCV_symbol_OnDemand(symbol, start_date_dt, end_date_dt, client=None, conn=None, cur=None, throttler=None):
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

    # Load symbol data
    symbol_data = bitfinex_load_symboldata()

    print(f'=== Fetching OHLCVs for symbol {symbol}')
    symbol_tasks.append(loop.create_task(bitfinex_fetchOHLCV_symbol(
        symbol_data=symbol_data,
        symbol=symbol,
        start_date=start_date_dt,
        time_frame=OHLCV_TIMEFRAME,
        limit=OHLCV_LIMIT,
        sort=1,
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
    asyncio.run(bitfinex_fetchOHLCV_all_symbols(datetime.datetime(2019, 1, 1)))

def run_OnDemand(symbol, start_date_dt, end_date_dt):
    asyncio.run(bitfinex_fetchOHLCV_symbol_OnDemand(symbol, start_date_dt, end_date_dt))


if __name__ == "__main__":
    run()


# def parse_ohlcv(ohlcv, symbol, base_id, quote_id, fetch_start_time):
#     '''
#     returns parsed ohlcv in a tuple
#     params:
#         `ohlcv`: list (ohlcv returned from OHLCV request)
#         `symbol`: string
#         `base_id`: string
#         `quote_id`: string
#         `fetch_start_time`: datetime obj - fetch starting time
#     '''

#     return (milliseconds_to_datetime(ohlcv[0]),
#             EXCHANGE_NAME,
#             symbol,
#             base_id,
#             quote_id,
#             ohlcv[1],
#             ohlcv[3],
#             ohlcv[4],
#             ohlcv[2],
#             ohlcv[5],
#             fetch_start_time
#     )

# async def bitfinex_fetchOHLCV_symbol_default(symbol_data, symbol, start_date, end_date, time_frame, limit, sort, bfx_client, psycopg2_conn, psycopg2_cursor, current_time, throttler):
#     '''
#     returns OHLCVs for a symbol using bitfinex's default python API
#     params:
#         `symbol_data`: dict (of symbol data)
#         `symbol`: string
#         `start_date`: int (milliseconds start time)
#         `end_date`: int (milliseconds end time)
#         `time_frame`: string
#         `limit`: int
#         `sort`: int (1 or -1)
#         `bfx_client`: bfx Client object
#         `psycopg2_conn`: Psycopg2 connection object
#         `psycopg2_cursor`: Psycopg2 cursor object
#         `current_time`: int (fetch starting time in milliseconds)
#         `throttler`: Throttler object from Throttler
#     '''

#     base_id = symbol_data[symbol]['base_id']
#     quote_id = symbol_data[symbol]['quote_id']
    
#     while start_date < current_time:        
#         async with throttler:
#             if current_time - start_date > 60000:
#                 ohlcv_section = OHLCV_SECTION_HIST
#             else:
#                 ohlcv_section = OHLCV_SECTION_LAST
#             ohlcvs = await bfx_client.rest.get_public_candles(
#                 symbol=symbol,
#                 start=start_date,
#                 end=end_date,
#                 section=ohlcv_section,
#                 tf=time_frame,
#                 limit=limit,
#                 sort=sort
#             )            
#             if isinstance(ohlcvs, list):
#                 try:
#                     if ohlcvs[-1][0] > start_date:
#                         start_date = ohlcvs[-1][0]
#                     else:
#                         start_date += 60000
#                     for ohlcv in ohlcvs[1:]:
#                         ohlcv = parse_ohlcv(ohlcv, symbol, base_id, quote_id)
#                         # print(f'=== Inserting into bitfinex_ohlcv values {ohlcv}')
#                         psycopg2_cursor.execute(
#                             "INSERT INTO bitfinex_ohlcv VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",ohlcv
#                         )
#                     psycopg2_conn.commit()
#                 except Exception as e:
#                     print(f' === {e}')
#                     break
#             else:
#                 print(f'=== Error in {ohlcvs}')
#                 break