# This module fetches bittrex 1-minute OHLCV data

import asyncio
import datetime
import httpx
import backoff
from typing import Any, Iterable
from common.config.constants import (
    REDIS_DELIMITER, OHLCVS_TABLE,
    OHLCVS_ERRORS_TABLE, DEFAULT_DATETIME_STR_QUERY
)
from common.helpers.datetimehelpers import (
    datetime_to_str, str_to_datetime, list_days_fromto
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
from fetchers.utils.asyncioutils import onbackoff, onsuccessgiveup
from fetchers.utils.ratelimit import GCRARateLimiter
from fetchers.utils.exceptions import (
    MaximumRetriesReached, UnsuccessfulDatabaseInsert
)
from fetchers.rest.base import BaseOHLCVFetcher


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
OHLCVS_BITTREX_TOFETCH_REDIS = "ohlcvs_tofetch_bittrex"
OHLCVS_BITTREX_FETCHING_REDIS = "ohlcvs_fetching_bittrex"
DATETIME_STR_FORMAT = "%Y-%m-%dT%H:%M:%S"
OHLCVS_CONSUME_BATCH_SIZE = 100

class BittrexOHLCVFetcher(BaseOHLCVFetcher):
    '''REST Fetcher for OHLCV from Bittrex
    '''
    def __init__(self, *args):
        super().__init__(*args, exchange_name = EXCHANGE_NAME)

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
        
        Saves it in self.symbol_data
        '''

        # self.symbol_data = {}
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
    def make_ohlcv_url(
            cls,
            symbol: str,
            interval: str,
            start_date: datetime.datetime
        ) -> tuple:
        '''
        Returns tuple of string of OHLCV url and historical indicator
        
        :params:
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
    def make_tofetch_params(
            cls,
            symbol: str,
            start_date: str,
            end_date: str,
            interval: str
        ) -> str:
        '''
        Makes tofetch params to feed into Redis to-fetch set
        
        :params:
            `symbol`: symbol string
            `start_date`: string representing datetime
                that complies to `DEFAULT_DATETIME_STR_QUERY`
            `end_date`: string representing datetime
                that complies to `DEFAULT_DATETIME_STR_QUERY`
            `interval`: string
        
        example:
            `BTC-USD;;2021-06-16T00:00:00;;2021-06-17T00:00:00;;MINUTE_1`
        '''

        return f'{symbol}{REDIS_DELIMITER}{start_date}{REDIS_DELIMITER}{end_date}{REDIS_DELIMITER}{interval}'

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
                    ohlcv['startsAt'],
                    EXCHANGE_NAME, base_id, quote_id,
                    round_decimal(ohlcv['open']),
                    round_decimal(ohlcv['high']),
                    round_decimal(ohlcv['low']),
                    round_decimal(ohlcv['close']),
                    round_decimal(ohlcv['volume'])
                ) for ohlcv in ohlcvs
            ]
        return ohlcvs_table_insert
    
    @classmethod
    def make_error_tuple(
            cls,
            symbol: str,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            interval: str,
            historical: str,
            resp_status_code: int,
            exception_class: str,
            exception_msg: str
        ) -> tuple:
        '''
        Returns a list that contains: a tuple to insert into the ohlcvs error table

        :params:
            `symbol`: string
            `start_date`: datetime obj of start date
            `end_date`: datetime obj of end date
            `interval`: string - timeframe; e.g., MINUTE_1
            `historical`: string - historical or not
            `resp_status_code`: int - response status code
            `exception_class`: string
            `exception_msg`: string
        '''
        
        return (
            (EXCHANGE_NAME, symbol, start_date, end_date,
            interval, historical, resp_status_code,
            str(exception_class),exception_msg),
        )

    @backoff.on_predicate(
        backoff.constant,
        lambda result: result[0] == 429,
        max_tries=12,
        on_backoff=onbackoff,
        on_success=onsuccessgiveup,
        on_giveup=onsuccessgiveup,
        interval=RATE_LIMIT_SECS_PER_MIN
    )
    async def _get_ohlcv_data(
            self,
            ohlcv_url: str,
            throttler: Any=None,
            exchange_name: str=EXCHANGE_NAME
        ) -> tuple:
        '''
        Gets ohlcv data based on url;
            Also backoffs conservatively by 60 secs
        
        Returns a tuple with:
            - http status (None if there's none),
            - ohlcvs (None if there's none),
            - exception type (None if there's none),
            - error message (None if there's none)

        :params:
            `ohlcv_url`: string - ohlcvs API url
            `throttler`: self.rate_limiter
            `exchange_name`: string - this exchange's name
        '''

        retries = 0
        while retries < HTTPX_DEFAULT_RETRIES:
            async with self.rate_limiter:
                try:
                    ohlcvs_resp = await self.async_httpx_client.get(ohlcv_url)
                    ohlcvs_resp.raise_for_status()
                    ohlcv_data = ohlcvs_resp.json()
                    return (
                        ohlcvs_resp.status_code,
                        ohlcv_data,
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
                except httpx.TimeoutException as exc:
                    await asyncio.sleep(1) # for now just 1 sec
                except Exception as exc:
                    return (
                        None,
                        None,
                        type(exc),
                        f'EXCEPTION: Request error while requesting {ohlcv_url}'
                    )
            retries += 1
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
        ) -> None:
        '''
        Gets and parses ohlcvs from consumed params
        
        :params:
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
        ohlcv_result = await self._get_ohlcv_data(
            ohlcv_url, throttler=self.rate_limiter, exchange_name=self.exchange_name
        )
        resp_status_code = ohlcv_result[0]
        ohlcvs = ohlcv_result[1]
        exc_type = ohlcv_result[2]
        exception_msg = ohlcv_result[3]

        # If exc_type is None (meaning no exception), process;
        #   Else, process the error
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

                    # Comment this out - not needed atm
                    # if insert_success:
                    #         self.redis_client.srem(self.fetching_key, params)
                    if not insert_success:
                        exc_type = UnsuccessfulDatabaseInsert
                        exception_msg = "EXCEPTION: Unsuccessful database insert"
                        error_tuple = self.make_error_tuple(
                            symbol, start_date, end_date, interval, historical,
                            resp_status_code, exc_type, exception_msg
                        )
                        psql_bulk_insert(
                            self.psql_conn,
                            error_tuple,
                            OHLCVS_ERRORS_TABLE,
                            insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
                        )
                # else:
                    # self.redis_client.srem(self.fetching_key, params) # not needed atm
            except Exception as exc:
                exc_type = type(exc)
                exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc}'
                self.logger.warning(exception_msg)
                error_tuple = self.make_error_tuple(
                    symbol, start_date, end_date, interval, historical,
                    resp_status_code, exc_type, exception_msg
                )
                psql_bulk_insert(
                    self.psql_conn,
                    error_tuple,
                    OHLCVS_ERRORS_TABLE,
                    insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
                )
        else:
            self.logger.warning(exception_msg)
            error_tuple = self.make_error_tuple(
                symbol, start_date, end_date, interval, historical,
                resp_status_code, exc_type, exception_msg
            )
            psql_bulk_insert(
                    self.psql_conn,
                    error_tuple,
                    OHLCVS_ERRORS_TABLE,
                    insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
            )
        
        # PSQL Commit
        self.psql_conn.commit()

    async def _init_tofetch_redis(
            self,
            symbols: Iterable,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            interval: str
        ) -> None:
        '''
        Initializes feeding params to Redis to-fetch set
        
        :params:
            `symbols`: iterable of symbols
            `start_date`: datetime obj
            `end_date`: datetime obj
            `interval`: string
        
        Feeds the following information:
            - key: `self.tofetch_key`
            - value: `symbol;;interval;;historical;;start_date_str;;end_date_str`
        
        example:
            `BTC-USD;;2021-06-16T00:00:00;;2021-06-17T00:00:00;;MINUTE_1`
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
                self.make_tofetch_params(
                    symbol, date_fmted, end_date_fmted, interval
                ) for symbol in symbols
            ]
            self.redis_client.sadd(OHLCVS_BITTREX_TOFETCH_REDIS, *params_list)
            
            # Asyncio sleep to release event loop for the consume-ohlcvs task
            await asyncio.sleep(
                RATE_LIMIT_SECS_PER_MIN / RATE_LIMIT_HITS_PER_MIN
            )
        self.feeding = False
        self.logger.info("Redis: Successfully initialized feeding params")

    async def _consume_ohlcvs_redis(self, update: bool=False) -> None:
        '''
        Consumes OHLCV parameters from the Redis to-fetch set
        '''

        # When start, move all params from fetching set to to-fetch set
        # Only create http client when consuming, hence the context manager
        # Keep looping if either:
        # - self.feeding or
        # - there are elements in to-fetch set or fetching set
        fetching_params = self.redis_client.spop(
            OHLCVS_BITTREX_FETCHING_REDIS,
            self.redis_client.scard(OHLCVS_BITTREX_FETCHING_REDIS)
        )
        if fetching_params:
            self.redis_client.sadd(
                OHLCVS_BITTREX_TOFETCH_REDIS, *fetching_params
            )
        async with httpx.AsyncClient(
            timeout=self.httpx_timout, limits=self.httpx_limits) as client:
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
                        OHLCVS_BITTREX_TOFETCH_REDIS, OHLCVS_CONSUME_BATCH_SIZE
                    )
                    if params_list:
                        self.redis_client.sadd(OHLCVS_BITTREX_FETCHING_REDIS, *params_list)
                        get_parse_tasks = [
                            self._get_and_parse_ohlcv(params, update) for params in params_list
                        ]
                        await asyncio.gather(*get_parse_tasks)

                        self.redis_client.srem(self.fetching_key, *params_list)

    async def _fetch_ohlcvs_symbols(
            self,
            symbols: list,
            start_date_dt: datetime.datetime,
            end_date_dt: datetime.datetime,
            update: bool=False
        ) -> None:
        '''
        Function to get OHLCVs of symbols
        
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
                symbols, start_date_dt, end_date_dt, OHLCV_INTERVAL
            ),
            self._consume_ohlcvs_redis(update)
        )
