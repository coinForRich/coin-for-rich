### This script fetches bitfinex 1-minute OHLCV data

import asyncio
import datetime
import psycopg2
import httpx
import backoff
import redis
import time
from asyncio_throttle import Throttler
from common.config.constants import *
from common.helpers.datetimehelpers import (
    datetime_to_milliseconds, milliseconds_to_datetime
)
from fetchers.helpers.dbhelpers import psql_bulk_insert
from fetchers.helpers.asynciohelpers import *
from fetchers.config.constants import *
from fetchers.config.queries import (
    PSQL_INSERT_IGNOREDUP_QUERY, MUTUAL_BASE_QUOTE_QUERY
)


EXCHANGE_NAME = "bitfinex"
BASE_CANDLE_URL = "https://api-pub.bitfinex.com/v2/candles"
PAIR_EXCHANGE_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"
LIST_CURRENCY_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:currency"
OHLCV_TIMEFRAME = "1m"
OHLCV_SECTION_HIST = "hist"
OHLCV_SECTION_LAST = "last"
OHLCV_LIMIT = 9500
RATE_LIMIT_HITS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN'][EXCHANGE_NAME]
RATE_LIMIT_SECS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']
OHLCVS_BITFINEX_TOFETCH_REDIS = "ohlcvs_bitfinex_tofetch"
OHLCVS_BITFINEX_FETCHING_REDIS = "ohlcvs_bitfinex_fetching"

class BitfinexOHLCVFetcher:
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
            if pair_ex_resp:
                pair_ex = pair_ex_resp.json()[0]
            if list_cur_resp:
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
    def make_ohlcv_url(cls, time_frame, symbol, limit, start_date_mls, end_date_mls, sort):
        '''
        returns tuple of OHLCV url and OHLCV section
        params:
            `time_frame`: string - time frame, e.g., 1m
            `symbol`: string - trading symbol, e.g., BTSE:USD
            `limit`: int - number limit of results fetched
            `start_date_mls`: int - datetime obj converted into milliseconds
            `end_date_mls`: int - datetime obj converted into milliseconds
            `sort`: int (1 or -1)

        example: https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTSE:USD/hist?limit=10000&start=1577836800000&sort=1
        '''

        # Has to check for hist or last of OHLCV section
        # Fetch historical data if time difference between now and start date is > 60k mls
        delta = datetime_to_milliseconds(datetime.datetime.now()) - start_date_mls
        if delta > 60000:
            ohlcv_section = OHLCV_SECTION_HIST
        else:
            ohlcv_section = OHLCV_SECTION_LAST
        
        symbol = cls.make_tsymbol(symbol)
        ohlcv_url = f"{BASE_CANDLE_URL}/trade:{time_frame}:{symbol}/{ohlcv_section}?limit={limit}&start={start_date_mls}&end={end_date_mls}&sort={sort}"

        return (ohlcv_url, ohlcv_section)
    
    @classmethod
    def make_tofetch_params(cls, symbol, start_date_mls, end_date_mls, time_frame, limit, sort):
        '''
        makes tofetch params to feed into Redis to-fetch set
        params:
            `symbol`: symbol string
            `start_date_mls`: datetime in millisecs
            `end_date_mls`: datetime in millisecs
            `time_frame`: string
            `limit`: int
            `sort`: int (1 or -1)
        e.g.:
            'BTCUSD;;1000000;;2000000;;1m;;9000;;1
        '''

        return f'{symbol}{REDIS_DELIMITER}{start_date_mls}{REDIS_DELIMITER}{end_date_mls}{REDIS_DELIMITER}{time_frame}{REDIS_DELIMITER}{limit}{REDIS_DELIMITER}{sort}'

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
                            EXCHANGE_NAME, base_id, quote_id,
                            ohlcv[1], ohlcv[3], ohlcv[4], ohlcv[2], ohlcv[5]
                        )
                    )
            else:
                ohlcvs_table_insert.append(
                    (
                            milliseconds_to_datetime(ohlcvs[0]),
                            EXCHANGE_NAME, base_id, quote_id,
                            ohlcvs[1], ohlcvs[3], ohlcvs[4], ohlcvs[2], ohlcvs[5]
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
        gets ohlcv data based on url
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
                    f'EXCEPTION: Request error while requesting {ohlcv_url}'
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
        start_date_mls = int(params_split[1])
        end_date_mls = int(params_split[2])
        time_frame = params_split[3]
        limit = params_split[4]
        sort = params_split[5]

        # Construct url and fetch
        base_id = self.symbol_data[symbol]['base_id']
        quote_id = self.symbol_data[symbol]['quote_id']

        ohlcv_url, ohlcv_section = self.make_ohlcv_url(
            time_frame, symbol, limit, start_date_mls, end_date_mls, sort
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
        # Why increment start_date_mls by 60000 * OHLCV_LIMIT:
        #   Because in each request we fetch at least `OHLCV_LIMIT`
        #   transaction-minutes. Thus, the next transaction-minute must
        #   be at least 60000 * OHLCV_LIMIT milliseconds away
        if exc_type is None:
            try:
                # Copy to PSQL if parsed successfully
                # Get the latest date in OHLCVS list,
                #   if latest date > start_date, update start_date
                ohlcvs_parsed = self.parse_ohlcvs(ohlcvs, base_id, quote_id, ohlcv_section)
                if ohlcvs_parsed:
                    psql_bulk_insert(
                        self.psql_conn, ohlcvs_parsed, OHLCVS_TABLE, PSQL_INSERT_IGNOREDUP_QUERY
                    )
                    ohlcvs_last_date = datetime_to_milliseconds(ohlcvs_parsed[-1][0])
                    if ohlcvs_last_date > start_date_mls:
                        start_date_mls = ohlcvs_last_date
                    else:
                        start_date_mls += (60000 * OHLCV_LIMIT)
                else:
                    start_date_mls += (60000 * OHLCV_LIMIT)
            except Exception as exc:
                exc_type = type(exc)
                exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc}'
                print(exception_msg)
                error_tuple = self.make_error_tuple(symbol, start_date_mls, end_date_mls, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg)
                psql_bulk_insert(
                    self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE, PSQL_INSERT_IGNOREDUP_QUERY
                )
                start_date_mls += (60000 * OHLCV_LIMIT)
        else:
            print(exception_msg)
            error_tuple = self.make_error_tuple(symbol, start_date_mls, end_date_mls, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg)
            psql_bulk_insert(
                self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE,
                PSQL_INSERT_IGNOREDUP_QUERY
            )
            start_date_mls += (60000 * OHLCV_LIMIT)
        
        # PSQL Commit
        self.psql_conn.commit()

        # Also make more params for to-fetch set
        if start_date_mls < end_date_mls:
            return self.make_tofetch_params(
                symbol, start_date_mls, end_date_mls, time_frame, limit, sort
            )
        else:
            return None

    async def init_tofetch_redis(self, symbols, start_date, end_date, time_frame, limit, sort):
        '''
        Initializes feeding params to Redis to-fetch set
        params:
            `symbols`: iterable of symbols
            `start_date`: datetime obj
            `end_date`: datetime obj
            `time_frame`: string
            `limit`: int
            `sort`: int (1 or -1)
        e.g.:
            'BTCUSD;;1000000;;2000000;;1m;;9000;;1
        feeds the following information:
        - key: OHLCVS_BITFINEX_TOFETCH_REDIS
        - value: symbol;;start_date_mls;;end_date_mls;;time_frame;;limit;;sort
        '''
        # Set feeding status
        # Convert datetime with tzinfo to non-tzinfo, if any
        self.feeding = True
        start_date = start_date.replace(tzinfo=None)
        end_date = end_date.replace(tzinfo=None)
        start_date_mls = datetime_to_milliseconds(start_date)
        end_date_mls = datetime_to_milliseconds(end_date)

        # The maximum list of symbols is short enough to feed
        #   to Redis in a batch (~300 symbols total as of June 2021)
        # Finally reset feeding status
        params_list = [
            self.make_tofetch_params(symbol, start_date_mls, end_date_mls, time_frame, limit, sort) for symbol in symbols
        ]
        self.redis_client.sadd(OHLCVS_BITFINEX_TOFETCH_REDIS, *params_list)
        self.feeding = False
        print("Redis: Successfully initialized feeding params")

    async def consume_ohlcvs_redis(self):
        '''
        Consumes OHLCV parameters from the to-fetch Redis set
        '''

        # Move all params from fetching set to to-fetch set
        # Only create http client when consuming, hence the context manager
        # Keep looping and processing in batch if either:
        # - self.feeding or
        # - there are elements in to-fetch set or fetching set
        fetching_params = self.redis_client.spop(
            OHLCVS_BITFINEX_FETCHING_REDIS,
            self.redis_client.scard(OHLCVS_BITFINEX_FETCHING_REDIS)
        )
        if fetching_params:
            self.redis_client.sadd(
                OHLCVS_BITFINEX_TOFETCH_REDIS, *fetching_params
            )
        async with httpx.AsyncClient(timeout=None, limits=self.httpx_limits) as client:
            self.async_httpx_client = client
            while self.feeding or \
                self.redis_client.scard(OHLCVS_BITFINEX_TOFETCH_REDIS) > 0 \
                or self.redis_client.scard(OHLCVS_BITFINEX_FETCHING_REDIS) > 0:
                # Pop a batch of size `rate_limit` from Redis to-fetch set,
                #   send it to Redis fetching set
                # Add params in params list to Redis fetching set
                # New to-fetch params with new start dates will be results
                #   of `get_parse_tasks`
                #   Add these params to Redis to-fetch set, if not None
                # Finally, remove params list from Redis fetching set
                params_list = self.redis_client.spop(
                    OHLCVS_BITFINEX_TOFETCH_REDIS, RATE_LIMIT_HITS_PER_MIN
                )
                if params_list:
                    self.redis_client.sadd(OHLCVS_BITFINEX_FETCHING_REDIS, *params_list)
                    get_parse_tasks = [
                        self.get_and_parse_ohlcv(params) for params in params_list
                    ]
                    task_results = await asyncio.gather(*get_parse_tasks)
                    new_tofetch_params_notnone = [
                        params for params in task_results if params is not None
                    ]
                    if new_tofetch_params_notnone:
                        print("Redis: Adding more params to to-fetch with new start dates")
                        self.redis_client.sadd(
                            OHLCVS_BITFINEX_TOFETCH_REDIS, *new_tofetch_params_notnone
                        )
                    self.redis_client.srem(OHLCVS_BITFINEX_FETCHING_REDIS, *params_list)
    
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
                symbols, start_date_dt, end_date_dt, OHLCV_TIMEFRAME, OHLCV_LIMIT, 1
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

    def get_mutual_basequote(self):
        '''
        Returns the list of the 30 mutual base-quote symbols
        '''
        
        self.psql_cur.execute(MUTUAL_BASE_QUOTE_QUERY, (EXCHANGE_NAME,))
        results = self.psql_cur.fetchall()
        symbols = [result[0] for result in results]
        return symbols

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