# This module fetches bittrex 1-minute OHLCV data

import asyncio
import datetime
import psycopg2
import httpx
import backoff
import redis
from asyncio_throttle import Throttler
from common.config.constants import *
from common.helpers.datetimehelpers import (
    datetime_to_str, str_to_datetime, list_days_fromto
)
from fetchers.helpers.dbhelpers import psql_bulk_insert
from fetchers.helpers.asynciohelpers import *
from fetchers.config.constants import *
from fetchers.config.queries import (
    PSQL_INSERT_IGNOREDUP_QUERY, MUTUAL_BASE_QUOTE_QUERY
)


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
OHLCV_SECTION_HIST = "historical"
RATE_LIMIT_HITS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN'][EXCHANGE_NAME]
RATE_LIMIT_SECS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']
OHLCVS_BITTREX_TOFETCH_REDIS = "ohlcvs_bittrex_tofetch"
OHLCVS_BITTREX_FETCHING_REDIS = "ohlcvs_bittrex_fetching"
DATETIME_STR_FORMAT = "%Y-%m-%dT%H:%M:%S"

class BittrexOHLCVFetcher:
    def __init__(self):
        # Exchange info
        self.exchange_name = EXCHANGE_NAME

        # HTTPX client
        self.httpx_limits = httpx.Limits(
            max_connections=HTTPX_MAX_CONCURRENT_CONNECTIONS[EXCHANGE_NAME]
        )
        # self.async_httpx_client = httpx.AsyncClient(timeout=None, limits=httpx_limits)

        # Async throttler
        self.async_throttler = Throttler(
            rate_limit = 1,
            period = RATE_LIMIT_SECS_PER_MIN / RATE_LIMIT_HITS_PER_MIN
        )

        # Postgres connection
        # TODO: Not sure if this is needed
        self.psql_conn = psycopg2.connect(DBCONNECTION)
        self.psql_cur = self.psql_conn.cursor()

        # Redis client, startdate mls Redis key
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
    def make_ohlcv_url(cls, symbol, interval, start_date):
        '''
        returns tuple of string of OHLCV url and historical indicator
        params:
            `symbol`: string - symbol
            `interval`: string - interval type (see INTERVALS)
            `start_date`: datetime object
        
        example: https://api.bittrex.com/v3/markets/1INCH-USD/candles/MINUTE_1/historical/2019/01/01
        '''
        
        # Has to check for hist or recent of OHLCV historical param
        # Fetch historical data if time difference between now and start date is > 1 day
        delta = datetime.datetime.now() - start_date
        historical = 0
        if delta.days > 1:
            historical = OHLCV_SECTION_HIST

        if historical == OHLCV_SECTION_HIST:
            if interval == "MINUTE_1" or interval == "MINUTE_5":
                return (
                    f'{BASE_URL}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}/{start_date.day}',
                    historical
                )
            elif interval == "HOUR_1":
                return (
                    f'{BASE_URL}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}/{start_date.month}',
                    historical
                )
            elif interval == "DAY_1":
                return (
                    f'{BASE_URL}/markets/{symbol}/candles/{interval}/{historical}/{start_date.year}',
                    historical
                )
        return (
            f"{BASE_URL}/markets/{symbol}/candles/{interval}/recent",
            historical
        )

    @classmethod
    def make_tofetch_params(cls, symbol, start_date, end_date, interval):
        '''
        makes tofetch params to feed into Redis to-fetch set
        params:
            `symbol`: symbol string
            `start_date`: datetime obj or string representing datetime
            `end_date`: datetime obj or string representing datetime
            `interval`: string
        e.g.:
            'BTC-USD;;2021-06-16T00:00:00;;2021-06-17T00:00:00;;MINUTE_1'
        '''

        return f'{symbol}{REDIS_DELIMITER}{start_date}{REDIS_DELIMITER}{end_date}{REDIS_DELIMITER}{interval}'

    @classmethod
    def parse_ohlcvs(cls, ohlcvs, base_id, quote_id):
        '''
        returns rows of parsed ohlcvs
        
        params:
            `ohlcvs`: list of ohlcv dicts (returned from request)
            `base_id`: string
            `quote_id`: string
        '''

        # Ignore ohlcvs that are empty, do not raise error,
        #   as other errors are catched elsewhere
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
    def make_error_tuple(cls, symbol, start_date, end_date, interval, historical, resp_status_code, exception_class, exception_msg):
        '''
        returns a list that contains:
            - a tuple to insert into the ohlcvs error table

        params:
            `symbol`: string
            `start_date`: datetime obj of start date
            `end_date`: datetime obj of end date
            `interval`: string - timeframe; e.g., MINUTE_1
            `historical`: string - historical or not
            `resp_status_code`: int - response status code
            `exception_class`: string
            `exception_msg`: string
        '''
        
        return [
            (EXCHANGE_NAME,symbol,start_date,end_date,interval,historical, \
            resp_status_code,str(exception_class),exception_msg)
        ]

    @backoff.on_predicate(
        backoff.constant,
        lambda result: result[0] == 429,
        max_tries=12,
        on_backoff=onbackoff,
        on_success=onsuccessgiveup,
        on_giveup=onsuccessgiveup,
        interval=RATE_LIMIT_SECS_PER_MIN
    )
    async def get_ohlcv_data(self, ohlcv_url, throttler=None, exchange_name=None):
        '''
        gets ohlcv data based on url;
            also backoffs conservatively by 60 secs
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

        async with self.async_throttler:
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
            except Exception as exc:
                return (
                    None,
                    None,
                    type(exc),
                    f'EXCEPTION: Request error while requesting {exc.request.url}'
                )

    async def get_and_parse_ohlcv(self, params):
        '''
        Gets and parses ohlcvs from consumed params
        
        params:
            `params`: params consumed from Redis to-fetch set
        '''

        # Extract params
        params_split = params.split(REDIS_DELIMITER)
        symbol = params_split[0]
        start_date = str_to_datetime(params_split[1], DEFAULT_DATETIME_STR_QUERY)
        end_date = str_to_datetime(params_split[2], DEFAULT_DATETIME_STR_QUERY)
        interval = params_split[3]

        # Construct url and fetch
        base_id = self.symbol_data[symbol]['base_id']
        quote_id = self.symbol_data[symbol]['quote_id']

        ohlcv_url, historical = self.make_ohlcv_url(
            symbol, interval, start_date
        )
        ohlcv_result = await self.get_ohlcv_data(
            ohlcv_url, throttler=self.async_throttler, exchange_name=EXCHANGE_NAME
        )
        resp_status_code = ohlcv_result[0]
        ohlcvs = ohlcv_result[1]
        exc_type = ohlcv_result[2]
        exception_msg = ohlcv_result[3]

        # If exc_type is None (meaning no exception), process;
        #   Else, process the error
        if exc_type is None:
            try:
                # Copy to PSQL if parsed successfully
                # Get the latest date in OHLCVS list,
                #   if latest date > start_date, update start_date
                ohlcvs_parsed = self.parse_ohlcvs(ohlcvs, base_id, quote_id)
                if ohlcvs_parsed:
                    psql_bulk_insert(
                        self.psql_conn, ohlcvs_parsed, OHLCVS_TABLE, PSQL_INSERT_IGNOREDUP_QUERY
                    )
            except Exception as exc:
                exc_type = type(exc)
                exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc}'
                print(exception_msg)
                error_tuple = self.make_error_tuple(
                    symbol, start_date, end_date, interval, historical, resp_status_code, exc_type, exception_msg
                )
                psql_bulk_insert(
                    self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE, PSQL_INSERT_IGNOREDUP_QUERY
                )
        else:
            print(exception_msg)
            error_tuple = self.make_error_tuple(
                symbol, start_date, end_date, interval, historical, resp_status_code, exc_type, exception_msg
            )
            psql_bulk_insert(
                self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE,
                PSQL_INSERT_IGNOREDUP_QUERY
            )
        
        # PSQL Commit
        self.psql_conn.commit()

        return None

    async def init_tofetch_redis(self, symbols, start_date, end_date, interval):
        '''
        Initializes feeding params to Redis to-fetch set
        
        params:
            `symbols`: list of symbols
            `start_date`: datetime obj
            `end_date`: datetime obj
            `interval`: string
        
        feeds the following information:
            - key: OHLCVS_BITTREX_TOFETCH_REDIS
            - value: symbol;;interval;;historical;;start_date_str;;end_date_str
        
        e.g.:
            'BTC-USD;;2021-06-16T00:00:00;;2021-06-17T00:00:00;;MINUTE_1'
        '''

        # Set feeding status
        # Convert datetime with tzinfo to non-tzinfo, if any
        # Also format end_date according to DEFAULT_DATETIME_STR_QUERY
        self.feeding = True
        start_date = start_date.replace(tzinfo=None)
        end_date = end_date.replace(tzinfo=None)
        end_date_fmted = datetime_to_str(end_date, DEFAULT_DATETIME_STR_QUERY)

        # Initial feed params to Redis set
        # Keep looping until start_date = end_date
        # while start_date < end_date:
        #     self.sadd_tofetch_redis(symbol, start_date, end_date, interval)
        #     start_date += datetime.timedelta(days=DAYDELTAS[interval])
        # Finally reset feeding status
        for date in list_days_fromto(start_date, end_date):
            date_fmted = datetime_to_str(date, DEFAULT_DATETIME_STR_QUERY)
            params_list = [
                self.make_tofetch_params(symbol, date_fmted, end_date_fmted, interval) for symbol in symbols
            ]
            self.redis_client.sadd(OHLCVS_BITTREX_TOFETCH_REDIS, *params_list)
        self.feeding = False
        print("Redis: Successfully initialized feeding params")

    async def consume_ohlcvs_redis(self):
        '''
        Consumes OHLCV parameters from the to-fetch Redis set
        '''

        # Keep looping if either:
        # - self.feeding or
        # - there are elements in to-fetch set or fetching set
        async with httpx.AsyncClient(timeout=None, limits=self.httpx_limits) as client:
            self.async_httpx_client = client
            while self.feeding or \
                self.redis_client.scard(OHLCVS_BITTREX_TOFETCH_REDIS) > 0 \
                    or self.redis_client.scard(OHLCVS_BITTREX_FETCHING_REDIS) > 0:
                # Pop a batch of size `rate_limit` from Redis to-fetch set,
                #   send it to Redis fetching set
                # Add params in params list to Redis fetching set
                # New to-fetch params with new start dates will be results
                #   of `get_parse_tasks`
                #   Add these params to Redis to-fetch set, if not None
                # Finally, remove params list from Redis fetching set
                    params_list = self.redis_client.spop(
                        OHLCVS_BITTREX_TOFETCH_REDIS, RATE_LIMIT_HITS_PER_MIN
                    )
                    if params_list:
                        self.redis_client.sadd(OHLCVS_BITTREX_FETCHING_REDIS, *params_list)
                        get_parse_tasks = [
                            self.get_and_parse_ohlcv(params) for params in params_list
                        ]
                        await asyncio.gather(*get_parse_tasks)
                        self.redis_client.srem(OHLCVS_BITTREX_FETCHING_REDIS, *params_list)

    async def fetch_ohlcvs_symbols(self, symbols, start_date_dt, end_date_dt):
        '''
        Function to get OHLCVs of symbols
        
        params:
            `symbol`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
        '''

        # Set feeding status so the consume
        # function does not close immediately
        self.feeding = True

        # Asyncio gather 2 tasks:
        # - Init to-fetch
        # - Consume from Redis to-fetch
        await asyncio.gather(
            self.init_tofetch_redis(
                symbols, start_date_dt, end_date_dt, OHLCV_INTERVAL
            ),
            self.consume_ohlcvs_redis()
        )

    async def resume_fetch(self):
        '''
        Resumes fetching tasks if there're params inside Redis sets
        '''

        # Asyncio gather 1 task:
        # - Consume from Redis to-fetch
        await asyncio.gather(
            self.consume_ohlcvs_redis()
        )

    def fetch_symbol_data(self):
        rows = [
            (EXCHANGE_NAME, bq['base_id'], bq['quote_id'], symbol) \
            for symbol, bq in self.symbol_data.items()
        ]
        psql_bulk_insert(
            self.psql_conn, rows, SYMBOL_EXCHANGE_TABLE,
            PSQL_INSERT_IGNOREDUP_QUERY
        )

    def get_mutual_basequote(self):
        '''
        Returns a dict of the 30 mutual base-quote symbols
            in this form:
                {
                    'BTC-USD': {
                        'base_id': 'BTC',
                        'quote_id': 'USD'
                    }
                }
        '''
        
        self.psql_cur.execute(MUTUAL_BASE_QUOTE_QUERY, (EXCHANGE_NAME,))
        results = self.psql_cur.fetchall()
        ret = {}
        for result in results:
            ret[result[0]] = {
                'base_id': self.symbol_data[result[0]]['base_id'],
                'quote_id': self.symbol_data[result[0]]['quote_id']
            }
        return ret

    def run_fetch_ohlcvs(self, symbols, start_date_dt, end_date_dt):
        '''
        Runs fetching OHLCVS
        params:
            `symbols`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
        '''

        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
        aio_set_exception_handler(loop)
        try:
            print("Run_fetch_ohlcvs: Fetching OHLCVS for indicated symbols")
            loop.run_until_complete(
                self.fetch_ohlcvs_symbols(symbols, start_date_dt, end_date_dt)
            )
        finally:
            print("Run_fetch_ohlcvs: Finished fetching OHLCVS for indicated symbols")
            loop.close()

    def run_fetch_ohlcvs_all(self, start_date_dt, end_date_dt):
        '''
        Runs the fetching OHLCVS for all symbols
        params:
            `symbols`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
        '''

        # Have to fetch symbol data first to
        # make sure it's up-to-date
        self.fetch_symbol_data()
        symbols = self.symbol_data.keys()

        self.run_fetch_ohlcvs(symbols, start_date_dt, end_date_dt)
        print("Run_fetch_ohlcvs_all: Finished fetching OHLCVS for all symbols")

    def run_resume_fetch(self):
        '''
        Runs the resuming of fetching tasks
        '''

        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
        aio_set_exception_handler(loop)
        try:
            print("Run_resume_fetch: Resuming fetching tasks from Redis sets")
            loop.run_until_complete(self.resume_fetch())
        finally:
            print("Run_resume_fetch: Finished fetching OHLCVS")
            loop.close()

    def run_fetch_ohlcvs_mutual_basequote(self, start_date_dt, end_date_dt):
        '''
        Runs the fetching of the 30 mutual base-quote symbols
        :params:
            `start_date_dt`: datetime obj
            `end_date_dt`: datetime obj
        '''
        # Have to fetch symbol data first to
        # make sure it's up-to-date
        self.fetch_symbol_data()
        
        symbols = self.get_mutual_basequote()
        self.run_fetch_ohlcvs(symbols, start_date_dt, end_date_dt)
        print("Run_fetch_ohlcvs_all: Finished fetching OHLCVS for common symbols")

    def close_connections(self):
        '''
        Close all connections (e.g., PSQL)
        '''

        self.psql_conn.close()
