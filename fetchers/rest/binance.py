### This module fetches binance 1-minute OHLCV data

import asyncio
import datetime
import psycopg2
import httpx
import backoff
import redis
import time
import random
from asyncio_throttle import Throttler
from common.config.constants import *
from common.helpers.datetimehelpers import (
    milliseconds_to_datetime, datetime_to_milliseconds
)
from fetchers.helpers.dbhelpers import psql_bulk_insert
from fetchers.helpers.asynciohelpers import *
from fetchers.config.constants import *
from fetchers.config.queries import (
    PSQL_INSERT_IGNOREDUP_QUERY, MUTUAL_BASE_QUOTE_QUERY
)


URL = "https://api.binance.com/api/v3/klines?symbol=BTCTUSD&interval=1m&startTime=1357020000000&limit=1000"

EXCHANGE_NAME = "binance"
BASE_URL = "https://api.binance.com/api/v3"
BASE_URL_1 = "https://api1.binance.com/api/v3"
BASE_URL_2 = "https://api2.binance.com/api/v3"
BASE_URL_3 = "https://api3.binance.com/api/v3"
OHLCV_TIMEFRAME = "1m"
OHLCV_LIMIT = 1000
DEFAULT_WEIGHT_LIMIT = 1200
RATE_LIMIT_HITS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN'][EXCHANGE_NAME]
RATE_LIMIT_SECS_PER_MIN = THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']
OHLCVS_BINANCE_TOFETCH_REDIS = "ohlcvs_binance_tofetch"
OHLCVS_BINANCE_FETCHING_REDIS = "ohlcvs_binance_fetching"
OHLCVS_STARTDATEMLS_REDIS = "binance_startdate_mls"


class RequestWeightManager:
    def __init__(
        self,
        weight_limit=DEFAULT_WEIGHT_LIMIT,
        duration=RATE_LIMIT_SECS_PER_MIN
    ):
        self.full_weight_limit = weight_limit
        self.duration = duration
        self.weight_limit = weight_limit
        self.timestamp = None

        # Weights of different requests
        self.rw_1 = 1
        self.rw_10 = 10
    
    def reset_weight_limit(self, time):
        if time - self.timestamp > self.duration:
            self.weight_limit = self.full_weight_limit

    def reset(self):
        print(f"Current weight limit is {self.weight_limit}")
        now = time.monotonic()
        if not self.timestamp:
            self.timestamp = now
        elif now - self.timestamp > self.duration:
            self.reset_weight_limit(now)
            self.timestamp = now

    def weight_ten(self):
        while True:
            self.reset()
            if self.weight_limit >= self.rw_10:
                self.weight_limit -= self.rw_10
                break
            else:
                now = time.monotonic()
                time.sleep(self.duration - (now - self.timestamp))

    async def aweight_one(self):
        '''
        To be inserted at the very beginning of a request function
        that costs 1 weight units
        '''

        while True:
            self.reset()
            if self.weight_limit >= self.rw_1:
                self.weight_limit -= self.rw_1
                break
            else:
                now = time.monotonic()
                await asyncio.sleep(self.duration - (now - self.timestamp))
    
    async def aweight_ten(self):
        '''
        To be inserted at the very beginning of a request function
        that costs 10 weight units
        '''

        while True:
            self.reset()
            if self.weight_limit >= self.rw_10:
                self.weight_limit -= self.rw_10
                break
            else:
                now = time.monotonic()
                await asyncio.sleep(self.duration - (now - self.timestamp))

    
class BinanceOHLCVFetcher:
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

        # Request weight manager
        self.rw_man = RequestWeightManager()

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
                'ETHBTC': {
                    'base_id': "ETH",
                    'quote_id': "BTC"
                },
                'some_other_symbol': {
                    'base_id': "ABC",
                    'quote_id': "XYZ"
                }
                ...
            }
        saves it in self.symbol_data
        '''

        # Only needs a temporary httpx client
        # This code can block (non-async) as it's needed for future fetching
        # Only processes trading symbols
        self.symbol_data = {}
        with httpx.Client(timeout=None) as client:
            self.rw_man.weight_ten()
            exch_info_resp = client.get(f'{BASE_URL}/exchangeInfo') \
                or client.get(f'{BASE_URL_1}/exchangeInfo') \
                or client.get(f'{BASE_URL_2}/exchangeInfo') \
                or client.get(f'{BASE_URL_3}/exchangeInfo')
            if exch_info_resp:
                symbol_info = exch_info_resp.json()['symbols']
            for symbol_dict in symbol_info:
                if symbol_dict['status'] == "TRADING":
                    self.symbol_data[symbol_dict['symbol']] = {
                        'base_id': symbol_dict['baseAsset'],
                        'quote_id': symbol_dict['quoteAsset']
                    }

    @classmethod
    def make_ohlcv_url(cls, interval, symbol, limit, start_date_mls):
        '''
        returns tuple of OHLCV url options
        :params:
            `interval`: string - interval, e.g., 1m
            `symbol`: string - trading symbol, e.g., BTCTUSD
            `limit`: int - number limit of results fetched
            `start_date_mls`: int - datetime obj converted into milliseconds

        example: "https://api.binance.com/api/v3/klines?symbol=BTCTUSD&interval=1m&startTime=1357020000000&limit=1000"
        '''

        return (
            f"{BASE_URL}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}",
            f"{BASE_URL_1}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}",
            f"{BASE_URL_2}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}",
            f"{BASE_URL_3}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}"
        )
    
    @classmethod
    def make_tofetch_params(cls, symbol, start_date_mls, end_date_mls, interval, limit):
        '''
        makes tofetch params to feed into Redis to-fetch set
        :params:
            `symbol`: symbol string
            `start_date_mls`: datetime in millisecs
            `end_date_mls`: datetime in millisecs
            `interval`: string
            `limit`: int
        e.g.:
            'BTCTUSD;;1000000;;2000000;;1m;;1000'
        '''

        return f'{symbol}{REDIS_DELIMITER}{start_date_mls}{REDIS_DELIMITER}{end_date_mls}{REDIS_DELIMITER}{interval}{REDIS_DELIMITER}{limit}'

    @classmethod
    def parse_ohlcvs(cls, ohlcvs, base_id, quote_id):
        '''
        returns a list of rows of parsed ohlcv
        :params:
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
                        milliseconds_to_datetime(ohlcv[0]),
                        EXCHANGE_NAME, base_id, quote_id,
                        float(ohlcv[1]), float(ohlcv[2]),
                        float(ohlcv[3]), float(ohlcv[4]), float(ohlcv[5])
                    )
                )
            return ohlcvs_table_insert
        else:
            return None

    @classmethod
    def make_error_tuple(cls, symbol, start_date, end_date, interval, resp_status_code, exception_class, exception_msg, ohlcv_section=None):
        '''
        returns a list that contains:
            a tuple to insert into the ohlcvs error table
        :params:
            `symbol`: string
            `start_date`: datetime obj of start date
            `end_date`: datetime obj of end date
            `time_frame`: string
            `resp_status_code`: int - response status code
            `exception_class`: string
            `exception_msg`: string
            `ohlcv_section`: default None
        '''

        # Convert start_date and end_date to datetime obj if needed;
        # Because timestamps in binance are in mls
        if not isinstance(start_date, datetime.datetime):
            start_date = milliseconds_to_datetime(start_date)
        if not isinstance(end_date, datetime.datetime):
            end_date = milliseconds_to_datetime(end_date)

        return [
            (EXCHANGE_NAME,symbol,start_date,end_date,
                interval,ohlcv_section, resp_status_code,
                str(exception_class),exception_msg)
        ]

    def reset_backoff(self):
        '''
        resets self backoff attributes
        '''

        self.backoff_stt = None
        self.backoff_url = None
        self.backoff_time = None
        self.backoff_duration = None

    async def get_ohlcv_data(self, ohlcv_url):
        '''
        gets ohlcv data based on url
        returns tuple:
            (
                http status (None if there's none),
                ohlcvs (None if there's none),
                exception type (None if there's none),
                error message (None if there's none)
            )
        :params:
            `ohlcv_url`: string - ohlcv API url
            `throttler`: asyncio throttler obj
        '''
        
        # Wait for weight units to be available
        # Check common backoff status to collectively backoff
        #   or check if this ohlcv_url is the one that initialized
        #   the backoff status
        # If there's no 429/418 or ohlcv_url is the original one, continue request
        #   Else, sleep until end of backoff duration
        # Only backoff for max 12 times/retries
        retries = 0
        while retries < 12:
            await self.rw_man.aweight_one()
            if (self.backoff_stt != 429 and self.backoff_stt != 418) \
                or ohlcv_url == self.backoff_url:
                async with self.async_throttler:
                    try:
                        ohlcvs_resp = await self.async_httpx_client.get(ohlcv_url)
                        ohlcvs_resp.raise_for_status()
                        self.reset_backoff()
                        return (
                            ohlcvs_resp.status_code,
                            ohlcvs_resp.json(),
                            None,
                            None
                        )
                    except httpx.HTTPStatusError as exc:
                        resp_status_code = exc.response.status_code
                        if resp_status_code == 429 or resp_status_code == 418:
                            self.backoff_stt = resp_status_code
                            self.backoff_url = ohlcv_url
                            self.backoff_time = time.monotonic()
                            self.backoff_duration = exc.response.headers['Retry-After']
                            self.async_throttler.period *= 2
                            print(f"get_ohlcv_data: EXCEPTION: Backing off - setting throttler to 1 request every {round(self.async_throttler.period, 2)} seconds")
                            asyncio.sleep(self.backoff_duration)
                        else:
                            self.reset_backoff()
                            return (
                                resp_status_code,
                                None,
                                type(exc),
                                f'EXCEPTION: Response status code: {resp_status_code} while requesting {exc.request.url}'
                            )
                    except Exception as exc:
                        self.reset_backoff()
                        return (
                            None,
                            None,
                            type(exc),
                            f'EXCEPTION: Request error while requesting {exc.request.url}'
                        )
            else:
                asyncio.sleep(
                    min(self.backoff_duration - (time.monotonic() - self.backoff_time) + random.random(), RATE_LIMIT_SECS_PER_MIN)
                )
            retries += 1
        self.reset_backoff()
        return None

    async def get_and_parse_ohlcv(self, params):
        '''
        Gets and parses ohlcvs from consumed params
        :params:
            `params`: params consumed from Redis to-fetch set
            e.g., 'BTCTUSD;;1000000;;2000000;;1m;;1000'
        '''
          
        # Extract params
        params_split = params.split(REDIS_DELIMITER)
        symbol = params_split[0]
        start_date_mls = int(params_split[1])
        end_date_mls = int(params_split[2])
        interval = params_split[3]
        limit = params_split[4]

        # Construct url and fetch, try out
        # all url options
        base_id = self.symbol_data[symbol]['base_id']
        quote_id = self.symbol_data[symbol]['quote_id']

        ohlcv_urls = self.make_ohlcv_url(
            interval, symbol, limit, start_date_mls
        )
        ohlcv_result = await self.get_ohlcv_data(
            ohlcv_urls[0]
        ) \
        or await self.get_ohlcv_data(
            ohlcv_urls[1]
        ) \
        or await self.get_ohlcv_data(
            ohlcv_urls[2]
        ) \
        or await self.get_ohlcv_data(
            ohlcv_urls[3]
        )

        if ohlcv_result:
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
                    ohlcvs_parsed = self.parse_ohlcvs(ohlcvs, base_id, quote_id)
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
                    error_tuple = self.make_error_tuple(symbol, start_date_mls, end_date_mls, interval, resp_status_code, exc_type, exception_msg)
                    psql_bulk_insert(
                        self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE, PSQL_INSERT_IGNOREDUP_QUERY
                    )
                    start_date_mls += (60000 * OHLCV_LIMIT)
            else:
                print(exception_msg)
                error_tuple = self.make_error_tuple(symbol, start_date_mls, end_date_mls, interval, resp_status_code, exc_type, exception_msg)
                psql_bulk_insert(
                    self.psql_conn, error_tuple, OHLCVS_ERRORS_TABLE,
                    PSQL_INSERT_IGNOREDUP_QUERY
                )
                start_date_mls += (60000 * OHLCV_LIMIT)
            
            # PSQL Commit
            self.psql_conn.commit()
        else:
            start_date_mls += (60000 * OHLCV_LIMIT)

        # Also make more params for to-fetch set
        if start_date_mls < end_date_mls:
            return self.make_tofetch_params(
                symbol, start_date_mls, end_date_mls, interval, limit
            )
        else:
            return None

    async def init_tofetch_redis(self, symbols, start_date, end_date, interval, limit):
        '''
        Initializes feeding params to Redis to-fetch set
        params:
            `symbols`: iterable of symbols
            `start_date`: datetime obj
            `end_date`: datetime obj
            `time_frame`: string
            `limit`: int
        e.g.:
            'BTCTUSD;;1000000;;2000000;;1m;;1000
        feeds the following information:
        - key: OHLCVS_binance_TOFETCH_REDIS
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
        #   to Redis in a batch (~1200 symbols total as of June 2021)
        # Finally reset feeding status
        params_list = [
            self.make_tofetch_params(symbol, start_date_mls, end_date_mls, interval, limit) for symbol in symbols
        ]
        self.redis_client.sadd(OHLCVS_BINANCE_TOFETCH_REDIS, *params_list)
        self.feeding = False
        print("Redis: Successfully initialized feeding params")

    async def consume_ohlcvs_redis(self):
        '''
        Consumes OHLCV parameters from the to-fetch Redis set
        '''

        # Reset all self-backoff attributes
        # Move all params from fetching set to to-fetch set
        # Only create http client when consuming, hence the context manager
        # Keep looping and processing in batch if either:
        # - self.feeding or
        # - there are elements in to-fetch set or fetching set
        self.reset_backoff()        
        fetching_params = self.redis_client.spop(
            OHLCVS_BINANCE_FETCHING_REDIS,
            self.redis_client.scard(OHLCVS_BINANCE_FETCHING_REDIS)
        )
        if fetching_params:
            self.redis_client.sadd(
                OHLCVS_BINANCE_TOFETCH_REDIS, *fetching_params
            )
        async with httpx.AsyncClient(timeout=None, limits=self.httpx_limits) as client:
            self.async_httpx_client = client
            while self.feeding or \
                self.redis_client.scard(OHLCVS_BINANCE_TOFETCH_REDIS) > 0 \
                or self.redis_client.scard(OHLCVS_BINANCE_FETCHING_REDIS) > 0:
                # Pop a batch of size `rate_limit` from Redis to-fetch set,
                #   send it to Redis fetching set
                # Add params in params list to Redis fetching set
                # New to-fetch params with new start dates will be results
                #   of `get_parse_tasks`
                #   Add these params to Redis to-fetch set, if not None
                # Finally, remove params list from Redis fetching set
                params_list = self.redis_client.spop(
                    OHLCVS_BINANCE_TOFETCH_REDIS, RATE_LIMIT_HITS_PER_MIN
                )
                if params_list:
                    self.redis_client.sadd(OHLCVS_BINANCE_FETCHING_REDIS, *params_list)
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
                            OHLCVS_BINANCE_TOFETCH_REDIS, *new_tofetch_params_notnone
                        )
                    self.redis_client.srem(OHLCVS_BINANCE_FETCHING_REDIS, *params_list)
    
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
                symbols, start_date_dt, end_date_dt, OHLCV_TIMEFRAME, OHLCV_LIMIT
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
        Returns a dict of the 30 mutual base-quote symbols
            in this form:
                {
                    'ETHBTC': {
                        'base_id': 'ETH',
                        'quote_id': 'BTC'
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

    def run_fetch_ohlcvs_mutual_basequote(self, start_date_dt, end_date_dt):
        '''
        Runs the fetching of the 30 common base-quote symbols
        :params:
            `start_date_dt`: datetime obj
            `end_date_dt`: datetime obj
        '''
        # Have to fetch symbol data first to
        # make sure it's up-to-date
        self.fetch_symbol_data()

        self.psql_cur.execute(MUTUAL_BASE_QUOTE_QUERY, (EXCHANGE_NAME,))
        results = self.psql_cur.fetchall()
        symbols = [result[0] for result in results]
        self.run_fetch_ohlcvs(symbols, start_date_dt, end_date_dt)
        print("Run_fetch_ohlcvs_all: Finished fetching OHLCVS for common symbols")

    def close_connections(self):
        '''
        Close all connections (e.g., PSQL)
        '''

        self.psql_conn.close()