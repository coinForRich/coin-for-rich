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
OHLCVS_BITTREX_FETCHING_REDIS = "ohlcvs_bittrex_fetching"
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

        if historical == "historical":
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
            (EXCHANGE_NAME,symbol,start_date,end_date,interval,historical,\
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

    def sadd_tofetch_redis(self, symbol, start_date, end_date, interval):
        '''
        Adds params to Redis to-fetch set
        params:
            `symbol`: symbol string
            `start_date`: datetime obj
            `end_date`: datetime obj
            `interval`: string - interval type (e.g., MINUTE_1)
        e.g.:
            'BTC-USD;;MINUTE_1;;historical;;2021-06-16T00:00:00;;2021-06-17T00:00:00'
        '''
        
        # Fetch historical data if time difference between now and start date is > 1 day
        delta = datetime.datetime.now() - start_date
        if delta.days > 1:
            historical = "historical"
        else:
            historical = 0
        
        # Needs to serialize datetime obj to str
        start_date_str = datetime_to_str(start_date, DATETIME_STR_FORMAT)
        end_date_str = datetime_to_str(end_date, DATETIME_STR_FORMAT)

        self.redis_client.sadd(
            OHLCVS_BITTREX_TOFETCH_REDIS,
            f'{symbol}{REDIS_DELIMITER}{interval}{REDIS_DELIMITER}{historical}{REDIS_DELIMITER}{start_date_str}{REDIS_DELIMITER}{end_date_str}'
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
            - value: symbol;;interval;;historical;;start_date_str;;end_date_str
        e.g.:
            'BTC-USD;;MINUTE_1;;historical;;2021-06-16T00:00:00;;2021-06-17T00:00:00'
        '''

        # Set feeding status
        self.feeding = True

        # Convert datetime with tzinfo to non-tzinfo, if any
        start_date = start_date.replace(tzinfo=None)
        end_date = end_date.replace(tzinfo=None)

        # Initial feed params to Redis set
        # Keep looping until start_date = end_date
        while start_date < end_date:
            self.sadd_tofetch_redis(symbol, start_date, end_date, interval)
            start_date += datetime.timedelta(days=DAYDELTAS[interval])

        # Reset feeding status
        self.feeding = False
    

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
                (self.redis_client.scard(OHLCVS_BITTREX_TOFETCH_REDIS) > 0 \
                    or self.redis_client.scard(OHLCVS_BITTREX_FETCHING_REDIS) > 0):
                    async with self.async_throttler:
                        # Pop 1 from to-fetch Redis set
                        # Send it to fetching Redis set
                        params = self.redis_client.spop(OHLCVS_BITTREX_TOFETCH_REDIS)
                        if params:
                            self.redis_client.sadd(OHLCVS_BITTREX_FETCHING_REDIS, params)
                            # Extract params
                            params_split = params.split(REDIS_DELIMITER)
                            symbol = params_split[0]
                            interval = params_split[1]
                            historical = params_split[2]
                            start_date_str = params_split[3]
                            start_date_dt = str_to_datetime(start_date_str, DATETIME_STR_FORMAT)
                            end_date_str = params_split[4]
                            end_date_dt = str_to_datetime(end_date_str, DATETIME_STR_FORMAT)

                            # Construct url and fetch
                            base_id = self.symbol_data[symbol]['base_id']
                            quote_id = self.symbol_data[symbol]['quote_id']

                            ohlcv_url = self.make_ohlcv_url(
                                symbol, interval, historical, start_date_dt
                            )
                            ohlcv_result = await self.get_ohlcv_data(
                                ohlcv_url, throttler=self.async_throttler, exchange_name=EXCHANGE_NAME
                            )
                            resp_status_code = ohlcv_result[0]
                            ohlcvs = ohlcv_result[1]
                            exc_type = ohlcv_result[2]
                            exception_msg = ohlcv_result[3]
                            
                            # If exc_type is None, process
                            # Else, process the error
                            if exc_type is None:
                                try:
                                    ohlcvs_parsed = self.parse_ohlcvs(ohlcvs, base_id, quote_id)
                                    if ohlcvs_parsed:
                                        # psql_copy_from_csv(self.psql_conn, ohlcvs_parsed, OHLCVS_TABLE)
                                        print(f"Parsed ohlcvs, first row is like this {ohlcvs_parsed[0]}")
                                except Exception as exc:
                                    exc_type = type(exc)
                                    exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc} with original response as: {ohlcvs}'
                                    error_tuple = self.make_error_tuple(symbol, start_date_dt, end_date_dt, interval, historical, resp_status_code, exc_type, exception_msg)
                                    # psql_copy_from_csv(self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE)
                                    print(exception_msg)
                            else:
                                error_tuple = self.make_error_tuple(symbol, start_date_dt, end_date_dt, interval, historical, resp_status_code, exc_type, exception_msg)
                                # psql_copy_from_csv(self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE)
                                print(exception_msg)
                    
                    # Commit and remove params from fetching set
                    self.psql_conn.commit()
                    self.redis_client.srem(OHLCVS_BITTREX_FETCHING_REDIS, params)

    async def fetch_ohlcvs_symbols(self, symbols, start_date_dt, end_date_dt):
        '''
        Function to get OHLCVs of symbols
        params:
            `symbol`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
        '''

        # Asyncio tasks and exception handler
        loop = asyncio.get_event_loop()
        tasks = []
        aio_set_exception_handler(loop)

        # Create feeding task for each symbol
        for symbol in symbols:
            print(f"=== Fetching OHLCVs for symbol {symbol}")
            tasks.append(
                loop.create_task(
                    self.feed_ohlcvs_redis(
                        symbol=symbol,
                        start_date=start_date_dt,
                        end_date=end_date_dt,
                        interval=OHLCV_INTERVAL
                    )
                )
            )
        # Create consuming task
        tasks.append(loop.create_task(self.consume_ohlcvs_redis()))

        # Await tasks and set fetching to False
        await asyncio.wait(tasks)

    async def fetch_ohlcvs_all_symbols(self, start_date_dt, end_date_dt):
        '''
        Function to fetch OHLCVS for all trading symbols on Bittrex
        params:
            `start_date_dt`: datetime object (for starting date)
            `end_date_dt`: datetime object (for ending date)
        '''

        # Run tasks and set fetching to False
        symbols = self.symbol_data.keys()
        await asyncio.gather(
            self.fetch_ohlcvs_symbols(symbols, start_date_dt, end_date_dt)
        )

    def fetch_symbol_data(self):
        rows = [
            (EXCHANGE_NAME, bq['base_id'], bq['quote_id'], symbol) \
            for symbol, bq in self.symbol_data.items()
        ]
        psql_copy_from_csv(self.psql_conn, rows, SYMBOL_EXCHANGE_TABLE)

    def run_fetch_ohlcvs(self, symbols, start_date_dt, end_date_dt):
        '''
        Runs fetching OHLCVS
        '''

        asyncio.run(self.fetch_ohlcvs_symbols(symbols, start_date_dt, end_date_dt))

    def run_fetch_ohlcvs_all(self, start_date_dt, end_date_dt):
        '''
        Runs the fetching OHLCVS for all symbols
        '''

        asyncio.run(self.fetch_ohlcvs_all_symbols(start_date_dt, end_date_dt))
    
    def close_connections(self):
        '''
        Close all connections (e.g., PSQL)
        '''

        self.psql_conn.close()
