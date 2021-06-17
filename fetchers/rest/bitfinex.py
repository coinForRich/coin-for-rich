### This script fetches bitfinex 1-minute OHLCV data

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


EXCHANGE_NAME = "bitfinex"
BASE_CANDLE_URL = "https://api-pub.bitfinex.com/v2/candles"
PAIR_EXCHANGE_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"
LIST_CURRENCY_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:currency"
OHLCV_TIMEFRAME = "1m"
OHLCV_SECTION_HIST = "hist"
OHLCV_SECTION_LAST = "last"
OHLCV_LIMIT = 9500
RATE_LIMIT_HITS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN']['bitfinex']
RATE_LIMIT_SECS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']
OHLCVS_BITFINEX_TOFETCH_REDIS = "ohlcvs_bitfinex_tofetch"
OHLCVS_BITFINEX_FETCHING_REDIS = "ohlcvs_bitfinex_fetching"
OHLCVS_STARTDATEMLS_REDIS = "bitfinex_startdate_mls"

class BitfinexOHLCVFetcher:
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
        saves it in self.symbol_data
        '''

        self.symbol_data = {}
        # Only needs a temporary httpx client
        # This code can block (non-async) as it's needed for future fetching
        with httpx.Client(timeout=None) as client:
            pair_ex_resp = client.get(PAIR_EXCHANGE_URL)
            list_cur_resp = client.get(LIST_CURRENCY_URL)
            pair_ex = pair_ex_resp.json()[0]
            list_cur = list_cur_resp.json()[0]
            for symbol in sorted(pair_ex):
                # e.g., 1INCH:USD
                # And some extra work to extract base_id and quote_id
                self.symbol_data[symbol] = {}
                bq_candidates = []
                for currency in list_cur:
                    if "" in symbol.split(currency):
                        bq_candidates.append(currency)
                bq_candidates = sorted(bq_candidates, key=lambda x: len(x), reverse=False)
                first = bq_candidates.pop()
                first_split = symbol.split(first)
                if "" in first_split:
                    if first_split.index("") == 0:
                        self.symbol_data[symbol]['base_id'] = first
                        for second in bq_candidates:
                            if "" in first_split[-1].split(second):
                                self.symbol_data[symbol]['quote_id'] = second
                    else:
                        self.symbol_data[symbol]['quote_id'] = first
                        for second in bq_candidates:
                            if "" in first_split[0].split(second):
                                self.symbol_data[symbol]['base_id'] = second
    
    @classmethod
    def make_tsymbol(cls, symbol):
        '''
        returns appropriate trade symbol for bitfinex
        params:
            `symbol`: string (trading symbol, e.g., BTSE:USD)
        '''
        
        return f't{symbol}'

    @classmethod
    def make_ohlcv_url(cls, time_frame, symbol, section, limit, start_date_mls, end_date_mls, sort):
        '''
        returns OHLCV url: string
        params:
            `time_frame`: string - time frame, e.g., 1m
            `symbol`: string - trading symbol, e.g., BTSE:USD
            `section`: string - whether it's historical data or latest data
            `limit`: int - number limit of results fetched
            `start_date_mls`: int - datetime obj converted into milliseconds
            `end_date_mls`: int - datetime obj converted into milliseconds
            `sort`: int (1 or -1)

        example: https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTSE:USD/hist?limit=10000&start=1577836800000&sort=1
        '''
        
        symbol = cls.make_tsymbol(symbol)
        return f"{BASE_CANDLE_URL}/trade:{time_frame}:{symbol}/{section}?limit={limit}&start={start_date_mls}&end={end_date_mls}&sort={sort}"
    
    @classmethod
    def parse_ohlcvs(cls, ohlcvs, base_id, quote_id, ohlcv_section):
        '''
        returns a list of rows of parsed ohlcv
        note, in the ohlcv response that:
            if ohlcv_section is `hist`, ohlcvs will be list of lists
            if ohlcv_section is `last`, ohlcvs will be a list
        params:
            `ohlcvs`: list of ohlcv dicts (returned from request)
            `base_id`: string
            `quote_id`: string
            `ohlcv_section`: string
        '''

        # Ignore ohlcvs that are empty, do not raise error,
        # as other errors are catched elsewhere
        if ohlcvs:
            ohlcvs_table_insert = []
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
            return ohlcvs_table_insert
        else:
            return None

    @classmethod
    def make_error_tuple(cls, symbol, start_date, end_date, time_frame, ohlcv_section, resp_status_code, exception_class, exception_msg):
        '''
        returns a list that contains:
            - a tuple to insert into the ohlcvs error table
        params:
            `symbol`: string
            `start_date`: datetime obj of start date
            `end_date`: datetime obj of end date
            `time_frame`: string
            `ohlcv_section`: string
            `resp_status_code`: int - response status code
            `exception_class`: string
            `exception_msg`: string
        '''

        # Convert start_date and end_date to datetime obj if needed;
        # Because timestamps in Bitfinex are in mls
        if not isinstance(start_date, datetime.datetime):
            start_date = milliseconds_to_datetime(start_date)
        if not isinstance(end_date, datetime.datetime):
            end_date = milliseconds_to_datetime(end_date)

        return [
            (EXCHANGE_NAME,symbol,start_date,end_date,time_frame,ohlcv_section, \
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

    def sadd_tofetch_redis(self, symbol, start_date_mls, end_date_mls, time_frame, limit, sort):
        '''
        Adds params to Redis to-fetch set
        params:
            `symbol`: symbol string
            `start_date_mls`: datetime in millisecs
            `end_date_mls`: datetime in millisecs
            `time_frame`: string
            `limit`: int
            `sort`: int (1 or -1)
        e.g.:
            'BTCUSD;;1000000;;2000000;;1m;;hist;;9000;;1
        '''
        
        # Fetch historical data if time difference between now and start date is > 60k mls
        delta = datetime_to_milliseconds(datetime.datetime.now()) - start_date_mls
        if delta > 60000:
            ohlcv_section = OHLCV_SECTION_HIST
        else:
            ohlcv_section = OHLCV_SECTION_LAST
        self.redis_client.sadd(
            OHLCVS_BITFINEX_TOFETCH_REDIS,
            f'{symbol}{REDIS_DELIMITER}{start_date_mls}{REDIS_DELIMITER}{end_date_mls}{REDIS_DELIMITER}{time_frame}{REDIS_DELIMITER}{ohlcv_section}{REDIS_DELIMITER}{limit}{REDIS_DELIMITER}{sort}'
        )

    async def feed_ohlcvs_redis(self, symbol, start_date, end_date, time_frame, limit, sort):
        '''
        Initially creates OHLCV parameters and feeds to a Redis set to begin fetching;
        This Redis set and params are exclusively for this exchange
        This method serves as a "prime" for fetching
        params:
            `symbol`: symbol string
            `start_date`: datetime obj
            `end_date`: datetime obj
            `time_frame`: string
            `limit`: int
            `sort`: int (1 or -1)
        feeds the following information:
            - key: OHLCVS_BITFINEX_TOFETCH_REDIS
            - value: symbol;;start_date_mls;;end_date_mls;;time_frame;;ohlcv_section;;limit;;sort
        e.g.:
            'BTCUSD;;1000000;;2000000;;1m;;hist;;9000;;1
        '''

        # Set feeding status
        self.feeding = True

        # Convert datetime with tzinfo to non-tzinfo, if any
        start_date = start_date.replace(tzinfo=None)
        end_date = end_date.replace(tzinfo=None)

        # Initial feed params to Redis set
        start_date_mls = datetime_to_milliseconds(start_date)
        end_date_mls = datetime_to_milliseconds(end_date)
        self.sadd_tofetch_redis(
            symbol, start_date_mls, end_date_mls, time_frame, limit, sort
        )
        
        # Reset feeding status
        self.feeding = False

    async def consume_ohlcvs_redis(self):
        '''
        Consumes OHLCV parameters from the to-fetch Redis set
        Also continuously feeds itself with new params if start_date < end_date
        '''

        # Keep looping if either:
        # - self.feeding or
        # - there are elements in to-fetch set or fetching set
        async with httpx.AsyncClient(timeout=None, limits=self.httpx_limits) as client:
            self.async_httpx_client = client
            while self.feeding or \
                (self.redis_client.scard(OHLCVS_BITFINEX_TOFETCH_REDIS) > 0 \
                    or self.redis_client.scard(OHLCVS_BITFINEX_FETCHING_REDIS) > 0):
                async with self.async_throttler:
                    # Pop 1 from Redis set to fetch
                    # Send it to Redis fetching set
                    params = self.redis_client.spop(OHLCVS_BITFINEX_TOFETCH_REDIS)
                    if params:
                        self.redis_client.sadd(OHLCVS_BITFINEX_FETCHING_REDIS, params)
                        # Extract params
                        params_split = params.split(REDIS_DELIMITER)
                        symbol = params_split[0]
                        start_date_mls = int(params_split[1])
                        end_date_mls = int(params_split[2])
                        time_frame = params_split[3]
                        ohlcv_section = params_split[4]
                        limit = params_split[5]
                        sort = params_split[6]

                        # Construct url and fetch
                        base_id = self.symbol_data[symbol]['base_id']
                        quote_id = self.symbol_data[symbol]['quote_id']

                        ohlcv_url = self.make_ohlcv_url(
                            time_frame, symbol, ohlcv_section, limit, start_date_mls, end_date_mls, sort
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
                                ohlcvs_parsed = self.parse_ohlcvs(ohlcvs, base_id, quote_id, ohlcv_section)
                                # Copy to PSQL if parsed successfully
                                if ohlcvs_parsed:
                                    psql_copy_from_csv(self.psql_conn, ohlcvs_parsed, OHLCVS_TABLE)
                                    # Get the latest date in OHLCVS list
                                    if ohlcv_section == OHLCV_SECTION_HIST:
                                        ohlcvs_last_date = ohlcvs[-1][0]
                                    else:
                                        ohlcvs_last_date = ohlcvs[0]
                                    if ohlcvs_last_date > start_date_mls:
                                        start_date_mls = ohlcvs_last_date
                                    else:
                                        start_date_mls += 60000
                                else:
                                    start_date_mls += 60000
                            except Exception as exc:
                                exc_type = type(exc)
                                exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc} with original response as: {ohlcvs}'
                                error_tuple = self.make_error_tuple(symbol, start_date_mls, end_date_mls, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg)
                                psql_copy_from_csv(self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE)
                                print(exception_msg)
                                start_date_mls += 60000
                        else:
                            error_tuple = self.make_error_tuple(symbol, start_date_mls, end_date_mls, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg)
                            psql_copy_from_csv(self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE)
                            print(exception_msg)
                            start_date_mls += 60000
                        
                        # Commit and remove params from fetching set
                        self.psql_conn.commit()
                        self.redis_client.srem(OHLCVS_BITFINEX_FETCHING_REDIS, params)
                
                # Also feed more params to to-fetch set outside the throttler
                if start_date_mls < end_date_mls:
                    print("Redis: Adding more params to to-fetch with new start_date")
                    self.sadd_tofetch_redis(
                        symbol, start_date_mls, end_date_mls, time_frame, limit, sort
                    )
    
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
            print(f'=== Fetching OHLCVs for symbol {symbol}')
            tasks.append(
                loop.create_task(
                    self.feed_ohlcvs_redis(
                        symbol=symbol,
                        start_date=start_date_dt,
                        end_date=end_date_dt,
                        time_frame=OHLCV_TIMEFRAME,
                        limit=OHLCV_LIMIT,
                        sort=1
                    )
                )
            )
        # Create consuming task
        tasks.append(loop.create_task(self.consume_ohlcvs_redis()))
        
        # Await tasks and set fetching to False
        await asyncio.wait(tasks)
    
    async def fetch_ohlcvs_all_symbols(self, start_date_dt, end_date_dt):
        '''
        Function to fetch OHLCVS for all trading symbols on Bitfinex
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
