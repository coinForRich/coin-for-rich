# This script fetches bittrex 1-minute OHLCV data

import asyncio
import datetime
import psycopg2
import httpx
import backoff
import redis
from asyncio_throttle import Throttler
from fetchers.helpers.datetimehelpers import *
from fetchers.helpers.dbhelpers import psql_copy_from_csv
from fetchers.helpers.asynciohelpers import *
from fetchers.config.constants import *
from common.config.constants import *


# Bittrex returns:
#   1-min or 5-min time windows in a period of 1 day
#   1-hour time windows in a period of 31 days
#   1-day time windows in a period of 366 days
EXCHANGE_NAME = "bittrex"
BASE_URL = "https://api.bittrex.com/v3"
MARKET_URL = "https://api.bittrex.com/v3/markets"
OHLCV_INTERVALS = ["MINUTE_1", "MINUTE_5", "HOUR_1", "DAY_1"]
DAYDELTAS = {"MINUTE_1": 1, "MINUTE_5": 1, "HOUR_1": 31, "DAY_1": 366}
OHLCV_INTERVAL = "MINUTE_1"
RATE_LIMIT_HITS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN']['bittrex']
RATE_LIMIT_SECS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']
OHLCVS_BITTREX_TOFETCH_REDIS = "ohlcvs_bittrex_tofetch" 
DATETIME_STR_FORMAT = "%Y-%m-%dT%H:%M:%S"

class BittrexOHLCVFetcher:
    def __init__(self):
        # Exchange info
        self.exchange_name = EXCHANGE_NAME

        # HTTPX client
        self.httpx_limits = httpx.Limits(max_connections=HTTPX_MAX_CONCURRENT_CONNECTIONS)
        # self.async_httpx_client = httpx.AsyncClient(timeout=None, limits=httpx_limits)

        # Async throttler
        self.async_throttler = Throttler(
            rate_limit=RATE_LIMIT_HITS_PER_MIN, period=RATE_LIMIT_SECS_PER_MIN
        )

        # Postgres connection
        # TODO: Not sure if this is needed
        self.psql_conn = psycopg2.connect(DBCONNECTION)
        self.psql_cur = self.psql_conn.cursor()

        # Redis client, startdate mls Redis key
        print(f'Redis password: {REDIS_PASSWORD}')
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )

        # Load market data
        self.load_symbol_data()

        # Redis initial feeding status
        self.feeding = False

    def load_symbol_data(self):
        '''
        loads market data into a dict of this form:
            {
                '1INCH-USD': {
                    'base_id': "1INCH",
                    'quote_id': "USD"
                },
                'some_other_symbol': {
                    'base_id': "ABC",
                    'quote_id': "XYZ"
                }
                ...
            }
        saves it in self.symbol_data
        '''

        self.symbol_data = {}
        # Only needs a temporary client
        # This code can block (non-async) as it's needed for future fetching
        with httpx.Client(timeout=None) as client:
            markets_resp = client.get(MARKET_URL)
            market_data = markets_resp.json()
            for symbol_data in market_data:
                symbol = symbol_data['symbol']
                base_id = symbol_data['baseCurrencySymbol'].upper()
                quote_id = symbol_data['quoteCurrencySymbol'].upper()
                self.symbol_data[symbol] = {
                    'base_id': base_id,
                    'quote_id': quote_id
                }

    @classmethod
    def make_ohlcv_url(cls, symbol, interval, historical, start_date):
        '''
        returns OHLCV url: string
        params:
            `symbol`: string - symbol
            `interval`: string - interval type (see INTERVALS)
            `historical`: string - whether it's historical data
            `start_date`: datetime object
        example: https://api.bittrex.com/v3/markets/1INCH-USD/candles/MINUTE_1/historical/2019/01/01
        '''

        if historical:
            if interval == "MINUTE_1" or interval == "MINUTE_5":
                return f'{BASE_URL}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}/{start_date.day}'
            elif interval == "HOUR_1":
                return f'{BASE_URL}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}'
            elif interval == "DAY_1":
                return f'{BASE_URL}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}'
        return f"{BASE_URL}/markets/{symbol}/candles/{interval}/recent"

    @classmethod
    def parse_ohlcvs(cls, ohlcvs, base_id, quote_id):
        '''
        returns rows of parsed ohlcv
        params:
            `ohlcvs`: list of ohlcv dicts (returned from request)
            `base_id`: string
            `quote_id`: string
        '''

        # Ignore ohlcvs that are empty, do not raise error,
        # as other errors are catched elsewhere
        if ohlcvs:
            ohlcvs_table_insert = []
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

            return ohlcvs_table_insert
        else:
            return None
    
    @classmethod
    def make_error_tuple(cls, symbol, start_date, end_date, interval, ohlcv_section, resp_status_code, exception_class, exception_msg):
        '''
        returns a list that contains:
            - a tuple to insert into the ohlcvs error table
        params:
            `symbol`: string
            `start_date`: datetime obj of start date
            `end_date`: datetime obj of end date
            `time_frame`: string - timeframe; e.g., MINUTE_1
            `ohlcv_section`: string - historical or not
            `resp_status_code`: int - response status code
            `exception_class`: string
            `exception_msg`: string
        '''
        
        return [
            (EXCHANGE_NAME,symbol,start_date,end_date,interval,ohlcv_section,\
            resp_status_code,str(exception_class),exception_msg)
        ]

    @backoff.on_predicate(backoff.fibo, lambda result: result[0] == 429, max_tries=12, on_backoff=onbackoff, on_success=onsuccessgiveup, on_giveup=onsuccessgiveup)
    async def get_ohlcv_data(self, ohlcv_url, throttler=None, exchange_name=None):
        '''
        gets ohlcv data based on url
        returns tuple:
            (
                http status (None if there's none),
                ohlcvs (None if there's none),
                exception type (None if there's none),
                error message (None if there's none)

            )
        params:
            `ohlcv_url`: string - ohlcv API url
            `throttler`: asyncio-throttle obj
            `exchange_name`: string - this exchange's name
        '''

        try:
            ohlcvs_resp = await self.async_httpx_client.get(ohlcv_url)
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

    def sadd_tofetch_redis(self, symbol, start_date, interval):
        '''
        Adds params to Redis to-fetch set
        params:
            `symbol`: symbol string
            `start_date`: datetime obj
            `end_date`: datetime obj
            `interval`: string - interval type (e.g., MINUTE_1)
        e.g.:
            'BTC-USD;;MINUTE_1;;historical;;2021-06-16T00:00:00'
        '''
        
        # Fetch historical data if time difference between now and start date is > 1 day
        delta = datetime.datetime.now() - start_date
        if delta.days > 1:
            historical = "historical"
        else:
            historical = None
        
        # Needs to serialize datetime obj to str
        start_date_str = start_date.strftime(DATETIME_STR_FORMAT)

        self.redis_client.sadd(
            OHLCVS_BITTREX_TOFETCH_REDIS,
            f'{symbol}{REDIS_DELIMITER}{interval}{REDIS_DELIMITER}{historical}{REDIS_DELIMITER}{start_date_str}'
        )

    async def feed_ohlcvs_redis(self, symbol, start_date, end_date, interval):
        '''
        Initially creates OHLCV parameters and feeds to a Redis set to begin fetching;
        This Redis set and params are exclusively for this exchange
        This method serves as a "prime" for fetching
        params:
            `symbol`: symbol string
            `start_date`: datetime obj
            `end_date`: datetime obj
            `interval`: string
        feeds the following information:
            - key: OHLCVS_BITTREX_TOFETCH_REDIS
            - value: symbol;;interval;;historical;;start_date_str
        e.g.:
            'BTC-USD;;MINUTE_1;;historical;;2021-06-16T00:00:00'
        '''

        # Set feeding status
        self.feeding = True

        # Convert datetime with tzinfo to non-tzinfo, if any
        start_date = start_date.replace(tzinfo=None)

        # Initial feed params to Redis set
        # Loop until start_date = end_date
        while start_date < end_date:
            self.sadd_tofetch_redis(symbol, start_date, interval)
            start_date += datetime.timedelta(days=DAYDELTAS[interval])

        # Reset feeding status
        self.feeding = False
    

    async def bittrex_fetchOHLCV_symbol(symbol_data, symbol, start_date, end_date, interval, httpx_client, psycopg2_conn, psycopg2_cursor, throttler):
        '''
        custom function that fetches OHLCV for a symbol from start_date
        params:
            `symbol_data`: dict (of symbol data)
            `symbol`: string
            `start_date`: datetime obj
            `end_date`: datetime obj
            `interval`: string (for interval, e.g., "MINUTE_1")
            `httpx_client`: httpx Client object
            `psycopg2_conn`: connection object of psycopg2
            `psycopg2_cursor`: cursor object of psycopg2
            `throttler`: Throttler object from Throttler
        '''

        base_id = symbol_data['baseCurrencySymbol'].upper()
        quote_id = symbol_data['quoteCurrencySymbol'].upper()
        
        # Convert datetime with tzinfo to non-tzinfo
        start_date = start_date.replace(tzinfo=None)

        while start_date < end_date:
            delta = datetime.datetime.now() - start_date
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
                        error_tuple = make_error_tuple(symbol, start_date, end_date, interval, ohlcv_section, resp_status_code, exc_type, exception_msg)
                        psycopg2_cursor.execute(error_tuple[0], error_tuple[1])
                        print(exception_msg)
                else:
                    error_tuple = make_error_tuple(symbol, start_date, end_date, interval, ohlcv_section, resp_status_code, exc_type, exception_msg)
                    psycopg2_cursor.execute(error_tuple[0], error_tuple[1])
                    print(exception_msg)
                
                # Increment start_date by the length according to `interval`
                psycopg2_conn.commit()
                start_date += datetime.timedelta(days=DAYDELTAS[interval])

    async def bittrex_fetchOHLCV_all_symbols(start_date_dt, end_date_dt):
        '''
        Main function to fetch OHLCV for all trading symbols on Bittrex
        params:
            `start_date_dt`: datetime object (for starting date)
            `end_date_dt`: datetime object (for ending date)
        '''

        limits = httpx.Limits(max_connections=HTTPX_MAX_CONCURRENT_CONNECTIONS)
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

            for symbol_data in market_data:
                print(f"=== Fetching OHLCVs for symbol {symbol_data['symbol']}")
                symbol_tasks.append(loop.create_task(bittrex_fetchOHLCV_symbol(
                    symbol_data=symbol_data,
                    symbol=symbol_data['symbol'],
                    start_date=start_date_dt,
                    end_date=end_date_dt,
                    interval=OHLCV_INTERVAL,
                    httpx_client=client,
                    psycopg2_conn=conn,
                    psycopg2_cursor=cur,
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
        market_data = bittrex_loadAllSymbolsData()

        # Lookup symbol dict of this symbol
        symbol_data = None
        for sd in market_data:
            if sd['symbol'] == symbol:
                symbol_data = sd

        print(f"=== Fetching OHLCVs for symbol {symbol}")
        symbol_tasks.append(loop.create_task(bittrex_fetchOHLCV_symbol(
            symbol_data=symbol_data,
            symbol=symbol,
            start_date=start_date_dt,
            end_date=end_date_dt,
            interval=OHLCV_INTERVAL,
            httpx_client=client,
            psycopg2_conn=conn,
            psycopg2_cursor=cur,
            throttler=throttler
        )))
        await asyncio.wait(symbol_tasks)
        if self_cur:
            cur.close()
        if self_conn:
            conn.close()
        if self_httpx_c:
            await client.aclose()

    def bittrex_loadAllSymbolsData():
        '''
        returns market data with all symbol names
        '''

        with httpx.Client(timeout=None) as client:
            # Psycopg2
            conn = psycopg2.connect(DBCONNECTION)

            # Load market data
            markets_url = f'{BASE_URL}/markets'
            markets_resp = client.get(markets_url)
            market_data = markets_resp.json()

            # Load into PSQL
            to_symexch = []
            for symbol_data in market_data:
                symbol = symbol_data['symbol']
                base_id = symbol_data['baseCurrencySymbol'].upper()
                quote_id = symbol_data['quoteCurrencySymbol'].upper()
                to_symexch.append(
                    (
                        EXCHANGE_NAME,
                        base_id,
                        quote_id,
                        symbol
                    )
                )
            psql_copy_from_csv(conn, to_symexch, "symbol_exchange")
            conn.commit()
            conn.close()
            return market_data

    def run(start_date_dt, end_date_dt):
        '''
        fetches OHLCVs for all symbols
        '''

        asyncio.run(bittrex_fetchOHLCV_all_symbols(start_date_dt, end_date_dt))

    def run_OnDemand(symbol, start_date_dt, end_date_dt):
        '''
        fetches OHLCVs for a symbol based from a start date to an end date
        params:
            `symbol`: string
            `start_date_dt`: datetime obj
            `end_date_dt`: datetime obj
        '''

        asyncio.run(bittrex_fetchOHLCV_symbol_OnDemand(symbol, start_date_dt, end_date_dt))


if __name__ == "__main__":
    run()


# Parse datetime from Redis
# datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")