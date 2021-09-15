### This module fetches binance 1-minute OHLCV data

import asyncio
import datetime
import logging
import httpx
import redis
import time
import random
from typing import Iterable, Tuple, Union
from redis.exceptions import LockError
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, REDIS_DELIMITER,
    OHLCVS_TABLE, OHLCVS_ERRORS_TABLE
)
from common.helpers.datetimehelpers import (
    milliseconds_to_datetime,
    datetime_to_milliseconds, redis_time
)
from common.helpers.numbers import round_decimal
from fetchers.config.constants import (
    THROTTLER_RATE_LIMITS, OHLCV_UNIQUE_COLUMNS,
    OHLCV_UPDATE_COLUMNS, REST_RATE_LIMIT_REDIS_KEY,
    HTTPX_DEFAULT_RETRIES
)
from fetchers.config.queries import (
    PSQL_INSERT_IGNOREDUP_QUERY, PSQL_INSERT_UPDATE_QUERY    
)
from fetchers.helpers.dbhelpers import psql_bulk_insert
from fetchers.utils.ratelimit import GCRARateLimiter
from fetchers.utils.exceptions import (
    MaximumRetriesReached, UnsuccessfulDatabaseInsert
)
from fetchers.rest.base import BaseOHLCVFetcher


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
BACKOFF_STT_REDIS = "backoff_stt_binance" # Common Backoff status
BACKOFF_URL_REDIS = "backoff_url_binance" # Common Backoff url
BACKOFF_TIME_REDIS = "backoff_time_binance" # Common Backoff time
BACKOFF_DUR_REDIS = "backoff_dur_binance" # Common Backoff duration

OHLCVS_CONSUME_BATCH_SIZE = 500
# At httpx concurrent limit of 200, lag bug seems to be gone

LOCK_TIMEOUT_SECS = 5

class RequestWeightManager:
    '''
    Request weight manager specifically for Binance

    Uses Redis to manage request weight
    '''
    
    def __init__(
        self,
        weight_limit: int,
        period: int,
        redis_client: redis.Redis=None
    ):
        self.full_weight_limit = weight_limit
        self.period = period
        self.key_ts = f'weight_limit_timestamp_{EXCHANGE_NAME}'
        self.key_rw = f'weight_limit_value_{EXCHANGE_NAME}'

        # Redis client
        if not redis_client:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                username="default",
                password=REDIS_PASSWORD,
                decode_responses=True
            )
        self.redis_client = redis_client
    
    def _is_enough(self, weight: int) -> tuple:
        '''
        Checks if the request weight poll has enough for
            operation with `weight`
        '''
        
        # logging.info(f"Current request weight is {self.redis_client.get(self.key_rw)}")
        now = redis_time(self.redis_client)

        try:
            with self.redis_client.lock(
                f'lock:{self.key_ts}',
                timeout=LOCK_TIMEOUT_SECS,
                blocking_timeout=0.01
            ) as lock:
                # Initialize
                self.redis_client.setnx(self.key_ts, now)
                self.redis_client.setnx(self.key_rw, self.full_weight_limit)
                
                # Reset timestamp and request weight if `period` of time
                #   has passed since last timestamp
                if now - float(self.redis_client.get(self.key_ts)) > self.period:
                    self.redis_client.set(self.key_ts, now)
                    self.redis_client.set(self.key_rw, self.full_weight_limit)
                
                # Check if there is enough weight
                request_weight = int(self.redis_client.get(self.key_rw))
                if request_weight >= weight:
                    self.redis_client.decrby(self.key_rw, weight)
                    return (True, None)
                else:
                    return (
                        False,
                        self.period - (now - float(self.redis_client.get(self.key_ts)))
                    )
        except LockError:
            return (
                False,
                self.period - (now - float(self.redis_client.get(self.key_ts)))
            )
        except Exception as exc:
            logging.warning(f"RequestWeightManager: EXCEPTION: {exc}")

    def _wait(self, weight: int) -> None:
        while True:
            enough, wait_time = self._is_enough(weight)
            if enough:
                break
            time.sleep(wait_time)

    async def _await(self, weight: int) -> None:
        while True:
            enough, wait_time = self._is_enough(weight)
            if enough:
                break
            await asyncio.sleep(wait_time)

    def check(self, weight: int) -> None:
        '''
        To be inserted at the very beginning of a request function
            that costs `weight` weight units (non-async)
        '''

        self._wait(weight)
    
    async def acheck(self, weight: int) -> None:
        '''
        To be inserted at the very beginning of a request function
            that costs `weight` weight units
        '''

        await self._await(weight)


class BinanceOHLCVFetcher(BaseOHLCVFetcher):
    def __init__(self, *args):
        super().__init__(*args, exchange_name = EXCHANGE_NAME)

        # Request weight manager
        self.rw_manager = RequestWeightManager(
            DEFAULT_WEIGHT_LIMIT,
            RATE_LIMIT_SECS_PER_MIN,
            self.redis_client
        )

        # Rate limiter
        self.rate_limiter = GCRARateLimiter(
            REST_RATE_LIMIT_REDIS_KEY.format(exchange = EXCHANGE_NAME),
            1,
            RATE_LIMIT_SECS_PER_MIN / RATE_LIMIT_HITS_PER_MIN,
            redis_client = self.redis_client
        )

        # Load market data
        self._load_symbol_data()

    def _load_symbol_data(self) -> None:
        '''
        Loads market data into a dict of this form:
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
        
        Saves it in self.symbol_data
        '''

        # Only needs a temporary httpx client
        # This code can block (non-async) as it's needed for future fetching
        # Only processes trading symbols
        # self.symbol_data = {}
        with httpx.Client(timeout=None) as client:
            self.rw_manager.check(10)
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
    def make_ohlcv_url(
            cls,
            interval: str,
            symbol: str,
            limit: int,
            start_date_mls: int
        ) -> Tuple[str, str, str, str]:
        '''
        Returns tuple of OHLCV url options

        :params:
            `interval`: string - interval, e.g., 1m
            `symbol`: string - trading symbol, e.g., BTCTUSD
            `limit`: int - number limit of results fetched
            `start_date_mls`: int - datetime obj converted into milliseconds

        Note that binance does not distinguish historical url or not

        example: https://api.binance.com/api/v3/klines?symbol=BTCTUSD&interval=1m&startTime=1357020000000&limit=1000
        '''

        return (
            f"{BASE_URL}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}",
            f"{BASE_URL_1}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}",
            f"{BASE_URL_2}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}",
            f"{BASE_URL_3}/klines?symbol={symbol}&interval={interval}&startTime={start_date_mls}&limit={limit}"
        )
    
    @classmethod
    def make_tofetch_params(
            cls,
            symbol: str,
            start_date_mls: int,
            end_date_mls: int,
            interval: str,
            limit: int
        ) -> str:
        '''
        Makes tofetch params to feed into Redis to-fetch set
        
        :params:
            `symbol`: string - symbol
            `start_date_mls`: int - datetime in millisecs
            `end_date_mls`: int - datetime in millisecs
            `interval`: string - time interval
            `limit`: int - limit of data points
        example:
            'BTCTUSD;;1000000;;2000000;;1m;;1000'
        '''

        return f'{symbol}{REDIS_DELIMITER}{start_date_mls}{REDIS_DELIMITER}{end_date_mls}{REDIS_DELIMITER}{interval}{REDIS_DELIMITER}{limit}'

    @classmethod
    def parse_ohlcvs(
            cls,
            ohlcvs: Iterable,
            base_id: str,
            quote_id: str
        ) -> list:
        '''
        Returns a list of rows of parsed ohlcvs
        
        :params:
            `ohlcvs`: iterable of ohlcv dicts (returned from request)
            `base_id`: string
            `quote_id`: string
        '''

        # Ignore ohlcvs that are empty, do not raise error,
        #   as other errors are catched elsewhere
        ohlcvs_table_insert = []
        if ohlcvs:
            ohlcvs_table_insert = [
                (
                    milliseconds_to_datetime(ohlcv[0]),
                    EXCHANGE_NAME, base_id, quote_id,
                    round_decimal(ohlcv[1]),
                    round_decimal(ohlcv[2]),
                    round_decimal(ohlcv[3]),
                    round_decimal(ohlcv[4]),
                    round_decimal(ohlcv[5])
                ) for ohlcv in ohlcvs
            ]
        return ohlcvs_table_insert
        # else:
        #     return None

    @classmethod
    def make_error_tuple(
            cls,
            symbol: str,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            interval: str,
            resp_status_code: int,
            exception_class: str,
            exception_msg: str,
            ohlcv_section: str=None
        ) -> tuple:
        '''
        Returns a list that contains: a tuple to insert into the ohlcvs error table
        
        :params:
            `symbol`: string
            `start_date`: datetime obj of start date
            `end_date`: datetime obj of end date
            `interval`: string
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

        return (
            (EXCHANGE_NAME, symbol, start_date, end_date,
            interval, ohlcv_section, resp_status_code,
            str(exception_class),exception_msg),
        )

    def _reset_backoff(self):
        '''
        Resets Redis backoff attributes
        '''

        self.redis_client.delete(
            BACKOFF_STT_REDIS,
            BACKOFF_URL_REDIS,
            BACKOFF_TIME_REDIS,
            BACKOFF_DUR_REDIS
        )

    async def _get_ohlcv_data(self, ohlcv_url: str) -> tuple:
        '''
        Gets ohlcv data based on url;
            Also prepares to backoff before making request
        
        Returns a tuple with:
            - http status (None if there's none),
            - ohlcvs (None if there's none),
            - exception type (None if there's none),
            - error message (None if there's none)
        
        :params:
            `ohlcv_url`: string - ohlcv API url
            `throttler`: asyncio throttler obj
        '''
        
        retries = 0
        while retries < HTTPX_DEFAULT_RETRIES:
            await self.rw_manager.acheck(1)
            backoff_stt = self.redis_client.get(BACKOFF_STT_REDIS)
            backoff_url = self.redis_client.get(BACKOFF_URL_REDIS)
            backoff_duration = self.redis_client.get(BACKOFF_DUR_REDIS)
            backoff_time = self.redis_client.get(BACKOFF_TIME_REDIS)
            if (backoff_stt != "429" and backoff_stt != "418") \
                or ohlcv_url == backoff_url:
                async with self.rate_limiter:
                    try:
                        ohlcvs_resp = await self.async_httpx_client.get(ohlcv_url)
                        ohlcvs_resp.raise_for_status()
                        self._reset_backoff()
                        ohlcv_data = ohlcvs_resp.json()
                        return (
                            ohlcvs_resp.status_code,
                            ohlcv_data,
                            None,
                            None
                        )
                    except httpx.HTTPStatusError as exc:
                        resp_status_code = exc.response.status_code
                        if resp_status_code == 429 or resp_status_code == 418:
                            retry_after = exc.response.headers['Retry-After']

                            self.redis_client.set(BACKOFF_STT_REDIS, resp_status_code)
                            self.redis_client.set(BACKOFF_URL_REDIS, ohlcv_url)
                            self.redis_client.set(BACKOFF_DUR_REDIS, retry_after)
                            self.redis_client.set(
                                BACKOFF_TIME_REDIS, redis_time(self.redis_client)
                            )

                            self.logger.info(f"get_ohlcv_data: Backing off...")
                            await asyncio.sleep(float(retry_after))
                        else:
                            self._reset_backoff()
                            return (
                                resp_status_code,
                                None,
                                type(exc),
                                f'EXCEPTION: Response status code: {resp_status_code} while requesting {exc.request.url}'
                            )
                    except httpx.TimeoutException as exc:
                        await asyncio.sleep(1) # for now just 1 sec
                    except Exception as exc:
                        self._reset_backoff()
                        return (
                            None,
                            None,
                            type(exc),
                            f'EXCEPTION: Request error while requesting {ohlcv_url}'
                        )
            else:
                self.logger.info("get_ohlcv_data: Backing off...")
                if backoff_duration and backoff_time:
                    await asyncio.sleep(
                        min(float(backoff_duration) - (redis_time(self.redis_client) - float(backoff_time)) + 10 * random.random(), RATE_LIMIT_SECS_PER_MIN)
                    )
                else:
                    await asyncio.sleep(RATE_LIMIT_SECS_PER_MIN)
            retries += 1
        self._reset_backoff()
        return (
            None,
            None,
            MaximumRetriesReached,
            f'EXCEPTION: Maximum retries reached while requesting {ohlcv_url}'
        )

    async def _get_and_parse_ohlcv(
            self,
            params: str,
            update: bool=False
        ) -> Union[str, None]:
        '''
        Gets and parses ohlcvs from consumed params

        :params:
            `params`: params consumed from Redis to-fetch set
        '''
          
        # Extract params
        params_split = params.split(REDIS_DELIMITER)
        symbol = params_split[0]
        start_date_mls = int(params_split[1])
        end_date_mls = int(params_split[2])
        interval = params_split[3]
        limit = params_split[4]

        # Construct url and fetch
        #   Also try out all url options
        base_id = self.symbol_data[symbol]['base_id']
        quote_id = self.symbol_data[symbol]['quote_id']

        ohlcv_urls = self.make_ohlcv_url(
            interval, symbol, limit, start_date_mls
        )
        ohlcv_result = await self._get_ohlcv_data(
            ohlcv_urls[0]
        ) \
        or await self._get_ohlcv_data(
            ohlcv_urls[1]
        ) \
        or await self._get_ohlcv_data(
            ohlcv_urls[2]
        ) \
        or await self._get_ohlcv_data(
            ohlcv_urls[3]
        )

        # if ohlcv_result:
        # what the heck? why need this condition check?

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
        # Finally, remove params only in 2 cases:
        #   - insert is successful
        #   - empty ohlcvs from API
        if exc_type is None:
            try:
                # Copy to PSQL if parsed successfully
                # Get the latest date in OHLCVS list,
                #   if latest date > start_date, update start_date
                ohlcvs_parsed = self.parse_ohlcvs(ohlcvs, base_id, quote_id)
                if ohlcvs_parsed:
                    insert_success = False
                    if update:
                        insert_success = psql_bulk_insert(
                            self.psql_conn,
                            ohlcvs_parsed,
                            OHLCVS_TABLE,
                            insert_update_query = PSQL_INSERT_UPDATE_QUERY,
                            unique_cols = OHLCV_UNIQUE_COLUMNS,
                            update_cols = OHLCV_UPDATE_COLUMNS
                        )
                    else:
                        insert_success = psql_bulk_insert(
                            self.psql_conn,
                            ohlcvs_parsed,
                            OHLCVS_TABLE,
                            insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
                        )
                    
                    ohlcvs_last_date = datetime_to_milliseconds(ohlcvs_parsed[-1][0])
                    if ohlcvs_last_date > start_date_mls:
                        start_date_mls = ohlcvs_last_date
                    else:
                        start_date_mls += (60000 * OHLCV_LIMIT)

                    # Comment this out - not needed atm
                    # if insert_success:
                    #     self.redis_client.srem(self.fetching_key, params)
                    # Honestly this part sucks...
                    if not insert_success:
                        exc_type = UnsuccessfulDatabaseInsert
                        exception_msg = "EXCEPTION: Unsuccessful database insert"
                        error_tuple = self.make_error_tuple(
                            symbol, start_date_mls, end_date_mls, interval,
                            resp_status_code, exc_type, exception_msg
                        )
                        psql_bulk_insert(
                            self.psql_conn,
                            error_tuple,
                            OHLCVS_ERRORS_TABLE,
                            insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
                        )
                else:
                    start_date_mls += (60000 * OHLCV_LIMIT)
                    # self.redis_client.srem(self.fetching_key, params) # not needed atm
            except Exception as exc:
                exc_type = type(exc)
                exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc}'
                self.logger.warning(exception_msg)
                error_tuple = self.make_error_tuple(
                    symbol, start_date_mls, end_date_mls, interval,
                    resp_status_code, exc_type, exception_msg
                )
                psql_bulk_insert(
                    self.psql_conn,
                    error_tuple,
                    OHLCVS_ERRORS_TABLE,
                    insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
                )
                start_date_mls += (60000 * OHLCV_LIMIT)
        else:
            self.logger.warning(exception_msg)
            error_tuple = self.make_error_tuple(
                symbol, start_date_mls, end_date_mls, interval,
                resp_status_code, exc_type, exception_msg
            )
            psql_bulk_insert(
                self.psql_conn,
                error_tuple,
                OHLCVS_ERRORS_TABLE,
                insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
            )
            start_date_mls += (60000 * OHLCV_LIMIT)
        
        # PSQL Commit
        self.psql_conn.commit()
        
        # what the heck? why need this condition check?
        # else:
        #     start_date_mls += (60000 * OHLCV_LIMIT)

        # Also make more params for to-fetch set
        if start_date_mls < end_date_mls:
            return self.make_tofetch_params(
                symbol, start_date_mls, end_date_mls, interval, limit
            )
        else:
            return None

    async def _init_tofetch_redis(
            self,
            symbols: Iterable,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            interval: str,
            limit: int
        ) -> None:
        '''
        Initializes feeding params to Redis to-fetch set
        
        :params:
            `symbols`: iterable of symbols
            `start_date`: datetime obj
            `end_date`: datetime obj
            `interval`: string
            `limit`: int
        
        Feeds the following information:
            - key: `self.tofetch_key`
            - value: `symbol;;start_date_mls;;end_date_mls;;time_frame;;limit;;sort`
        
        example:
            `BTCTUSD;;1000000;;2000000;;1m;;1000`
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
            self.make_tofetch_params(
                symbol, start_date_mls, end_date_mls, interval, limit
            ) for symbol in symbols
        ]
        self.redis_client.sadd(self.tofetch_key, *params_list)
        self.feeding = False
        self.logger.info("Redis: Successfully initialized feeding params")

    async def _consume_ohlcvs_redis(self, update: bool=False) -> None:
        '''
        Consumes OHLCV parameters from the Redis to-fetch set
        '''

        # When start, move all [existing] params from fetching set to to-fetch set
        # Keep looping and processing in batch if either:
        # - self.feeding or
        # - there are elements in to-fetch set or fetching set
        fetching_params = self.redis_client.spop(
            self.fetching_key,
            self.redis_client.scard(self.fetching_key)
        )
        if fetching_params:
            self.redis_client.sadd(
                self.tofetch_key, *fetching_params
            )
        async with httpx.AsyncClient(
            timeout=self.httpx_timout, limits=self.httpx_limits) as client:
            self.async_httpx_client = client
            while self.feeding or \
                self.redis_client.scard(self.tofetch_key) > 0 \
                or self.redis_client.scard(self.fetching_key) > 0:
                # Pop a batch of size `rate_limit` from Redis to-fetch set,
                #   send it to Redis fetching set
                # Add params in params list to Redis fetching set
                # New to-fetch params with new start dates will be results
                #   of `get_parse_tasks`
                #   Add these params to Redis to-fetch set, if not None
                # Finally, remove params list from Redis fetching set
                params_list = self.redis_client.spop(
                    self.tofetch_key, OHLCVS_CONSUME_BATCH_SIZE
                )
                if params_list:
                    self.redis_client.sadd(self.fetching_key, *params_list)
                    get_parse_tasks = [
                        self._get_and_parse_ohlcv(params, update) for params in params_list
                    ]
                    task_results = await asyncio.gather(*get_parse_tasks)
                    new_tofetch_params = [
                        params for params in task_results if params is not None
                    ]
                    if new_tofetch_params:
                        self.logger.info(
                            "Redis: Adding more params to to-fetch with new start dates")
                        self.redis_client.sadd(
                            self.tofetch_key, *new_tofetch_params)
                       
                    self.redis_client.srem(self.fetching_key, *params_list)
    
    async def _fetch_ohlcvs_symbols(
            self,
            symbols: list,
            start_date_dt: datetime.datetime,
            end_date_dt: datetime.datetime,
            update: bool=False
        ) -> None:
        '''
        Function to fetch OHLCVs of symbols
        
        :params:
            `symbols`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
            `update`: boolean - whether to update when inserting
                to PSQL db
        '''

        # Set feeding status so the consume
        # function does not close immediately
        self.feeding = True

        # Asyncio gather 2 tasks:
        # - Init to-fetch
        # - Consume from Redis to-fetch
        await asyncio.gather(
            self._init_tofetch_redis(
                symbols, start_date_dt, end_date_dt, OHLCV_TIMEFRAME, OHLCV_LIMIT
            ),
            self._consume_ohlcvs_redis(update)
        )
