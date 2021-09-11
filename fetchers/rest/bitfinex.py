### This module fetches bitfinex 1-minute OHLCV data

import asyncio
import datetime
import httpx
import backoff
from typing import Any, Iterable, Literal, Tuple, Union
from common.config.constants import (
    REDIS_DELIMITER,
    OHLCVS_TABLE, OHLCVS_ERRORS_TABLE
)
from common.helpers.datetimehelpers import (
    datetime_to_milliseconds, milliseconds_to_datetime
)
from common.helpers.numbers import round_decimal
from fetchers.config.constants import (
    THROTTLER_RATE_LIMITS, OHLCV_UNIQUE_COLUMNS,
    OHLCV_UPDATE_COLUMNS, REST_RATE_LIMIT_REDIS_KEY
)
from fetchers.config.queries import (
    PSQL_INSERT_IGNOREDUP_QUERY, PSQL_INSERT_UPDATE_QUERY
)
from fetchers.helpers.dbhelpers import psql_bulk_insert
from fetchers.utils.asyncioutils import onbackoff, onsuccessgiveup
from fetchers.utils.ratelimit import GCRARateLimiter
from fetchers.rest.base import BaseOHLCVFetcher


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
OHLCVS_CONSUME_BATCH_SIZE = 500

class BitfinexOHLCVFetcher(BaseOHLCVFetcher):
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
        
        Saves it in `self.symbol_data`

        This looks like a mess...
        '''

        # self.symbol_data = {}
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
    def make_tsymbol(cls, symbol: str) -> str:
        '''
        Returns appropriate trade symbol for bitfinex
        
        :params:
            `symbol`: string (trading symbol, e.g., BTSE:USD)
        '''
        
        return f't{symbol}'

    @classmethod
    def make_ohlcv_url(
            cls,
            time_frame: str,
            symbol: str,
            limit: int,
            start_date_mls: int,
            end_date_mls: int,
            sort: Literal[1, -1]
        ) -> Tuple[str, str]:
        '''
        Returns tuple of OHLCV url and OHLCV section

        :params:
            `time_frame`: string - time frame, e.g,, 1m
            `symbol`: string - trading symbol, e.g., BTSE:USD
            `limit`: int - number limit of results fetched
            `start_date_mls`: int of milliseconds
            `end_date_mls`: int of milliseconds
            `sort`: int (1 or -1)

        example: https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTCUSD/hist?limit=10000&start=1577836800000&sort=1
        '''

        # Has to check for hist or last of OHLCV section
        # Fetch historical data if time difference between now and start date is > 60k mls
        delta = datetime_to_milliseconds(datetime.datetime.now()) - start_date_mls
        symbol = cls.make_tsymbol(symbol)

        if delta > 60000:
            return (
                f"{BASE_CANDLE_URL}/trade:{time_frame}:{symbol}/{OHLCV_SECTION_HIST}?limit={limit}&start={start_date_mls}&end={end_date_mls}&sort={sort}",
                OHLCV_SECTION_HIST
            )
        else:
            return (
                f"{BASE_CANDLE_URL}/trade:{time_frame}:{symbol}/{OHLCV_SECTION_LAST}?sort={sort}",
                OHLCV_SECTION_LAST
            )
    
    @classmethod
    def make_tofetch_params(
            cls,
            symbol: str,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            time_frame: str,
            limit: int,
            sort: Literal[1, -1]
        ) -> str:
        '''
        Makes tofetch params to feed into Redis to-fetch set
        
        :params:
            `symbol`: symbol string
            `start_date`: datetime obj
            `end_date`: datetime obj
            `time_frame`: string
            `limit`: int
            `sort`: int (1 or -1)
        
        Example:
            `BTCUSD;;1000000;;2000000;;1m;;9000;;1`
        '''

        # Convert start_date and end_date to milliseconds if needed
        if not isinstance(start_date, int):
            start_date = datetime_to_milliseconds(start_date)
        if not isinstance(end_date, int):
            end_date = datetime_to_milliseconds(end_date)
        
        return f'{symbol}{REDIS_DELIMITER}{start_date}{REDIS_DELIMITER}{end_date}{REDIS_DELIMITER}{time_frame}{REDIS_DELIMITER}{limit}{REDIS_DELIMITER}{sort}'

    @classmethod
    def parse_ohlcvs(
            cls,
            ohlcvs: Iterable,
            base_id: str,
            quote_id: str,
            ohlcv_section: str
        ) -> list:
        '''
        Returns a list of rows of parsed ohlcvs
        
        Note, in the ohlcv response from Bitfinex, that:
            - if ohlcv_section is `hist`, ohlcvs will be list of lists
            - if ohlcv_section is `last`, ohlcvs will be a list
        
        :params:
            `ohlcvs`: iterable of ohlcv data received from an API request
            `base_id`: string
            `quote_id`: string
            `ohlcv_section`: string
        '''

        # Ignore ohlcvs that are empty, do not raise error,
        #   as other errors are catched elsewhere
        ohlcvs_table_insert = []
        if ohlcvs:
            if ohlcv_section == OHLCV_SECTION_HIST:
                ohlcvs_table_insert = [
                    (
                        milliseconds_to_datetime(ohlcv[0]),
                        EXCHANGE_NAME, base_id, quote_id,
                        round_decimal(ohlcv[1]),
                        round_decimal(ohlcv[3]),
                        round_decimal(ohlcv[4]),
                        round_decimal(ohlcv[2]),
                        round_decimal(ohlcv[5])
                    ) for ohlcv in ohlcvs
                ]
            else:
                ohlcvs_table_insert = [
                    (
                        milliseconds_to_datetime(ohlcvs[0]),
                        EXCHANGE_NAME, base_id, quote_id,
                        ohlcvs[1], ohlcvs[3], ohlcvs[4], ohlcvs[2], ohlcvs[5]
                    )
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
            time_frame: str,
            ohlcv_section: str,
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
            `time_frame`: string - timeframe
            `ohlcv_section`: string - historical or recent
            `resp_status_code`: int - response status code
            `exception_class`: string
            `exception_msg`: string
        '''

        # Convert start_date and end_date to datetime obj if needed;
        #   as timestamps in Bitfinex are in mls
        if not isinstance(start_date, datetime.datetime):
            start_date = milliseconds_to_datetime(start_date)
        if not isinstance(end_date, datetime.datetime):
            end_date = milliseconds_to_datetime(end_date)

        return (
            (EXCHANGE_NAME, symbol, start_date, end_date,
            time_frame, ohlcv_section, resp_status_code,
            str(exception_class),exception_msg)
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
            Also backoffs conservatively by 60 secs (not sure it works as intended..)
        
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
        
        async with self.rate_limiter:
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
        time_frame = params_split[3]
        limit = params_split[4]
        sort = params_split[5]

        # Construct url and fetch
        base_id = self.symbol_data[symbol]['base_id']
        quote_id = self.symbol_data[symbol]['quote_id']

        ohlcv_url, ohlcv_section = self.make_ohlcv_url(
            time_frame, symbol, limit, start_date_mls, end_date_mls, sort
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
        # Why increment start_date_mls by 60000 * OHLCV_LIMIT:
        #   Because in each request we fetch at least `OHLCV_LIMIT`
        #   transaction-minutes. Thus, the next transaction-minute must
        #   be at least 60000 * OHLCV_LIMIT milliseconds away
        # Copy to PSQL if parsed successfully
        # Perform update if `update` is True
        # Get the latest date in OHLCVS list,
        #   if latest date > start_date, update start_date
        # Finally, remove params only in 2 cases:
        #   - insert is successful
        #   - empty ohlcvs from API
        if exc_type is None:
            try:
                ohlcvs_parsed = self.parse_ohlcvs(
                    ohlcvs, base_id, quote_id, ohlcv_section)
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

                    if insert_success:
                        self.redis_client.srem(self.fetching_key, params)
                else:
                    start_date_mls += (60000 * OHLCV_LIMIT)
                    self.redis_client.srem(self.fetching_key, params)
            except Exception as exc:
                exc_type = type(exc)
                exception_msg = f'EXCEPTION: Error while processing ohlcv response: {exc}'
                self.logger.warning(exception_msg)
                error_tuple = self.make_error_tuple(
                    symbol, start_date_mls, end_date_mls, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg
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
                symbol, start_date_mls, end_date_mls, time_frame, ohlcv_section, resp_status_code, exc_type, exception_msg
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

        # Also make more params for to-fetch set
        if start_date_mls < end_date_mls:
            return self.make_tofetch_params(
                symbol, start_date_mls, end_date_mls, time_frame, limit, sort
            )
        else:
            return None

    async def _init_tofetch_redis(
            self,
            symbols: Iterable,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            time_frame: str,
            limit: int,
            sort: Literal[1, -1]
        ) -> None:
        '''
        Initializes feeding params to Redis to-fetch set
        
        :params:
            `symbols`: iterable of symbols
            `start_date`: datetime obj
            `end_date`: datetime obj
            `time_frame`: string
            `limit`: int
            `sort`: int (1 or -1)
        
        Feeds the following information:
            - key: `self.tofetch_key`
            - value: `symbol;;start_date_mls;;end_date_mls;;time_frame;;limit;;sort`
        
        example:
            `BTCUSD;;1000000;;2000000;;1m;;9000;;1`
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
            self.make_tofetch_params(
                symbol, start_date_mls, end_date_mls, time_frame, limit, sort
            ) for symbol in symbols
        ]
        self.redis_client.sadd(self.tofetch_key, *params_list)
        self.feeding = False
        self.logger.info("Redis: Successfully initialized feeding params")

    async def _consume_ohlcvs_redis(self, update: bool=False) -> None:
        '''
        Consumes OHLCV parameters from Redis to-fetch set
        '''

        # When start, move all [existing] params from fetching set to to-fetch set
        # Keep looping and processing in batch if either:
        #   - self.feeding or
        #   - there are elements in to-fetch set or fetching set
        fetching_params = self.redis_client.spop(
            self.fetching_key,
            self.redis_client.scard(self.fetching_key)
        )
        if fetching_params:
            self.redis_client.sadd(
                self.tofetch_key, *fetching_params
            )
        
        # Pop a batch of size `rate_limit` from Redis to-fetch set,
        #   send it to Redis fetching set
        # Add params in params list to Redis fetching set
        # New to-fetch params with new start dates will be results
        #   of `get_parse_tasks`
        #   Add these params to Redis to-fetch set, if not None
        # Finally, remove params list from Redis fetching set
        async with httpx.AsyncClient(timeout=None, limits=self.httpx_limits) as client:
            self.async_httpx_client = client
            while self.feeding or \
                self.redis_client.scard(self.tofetch_key) > 0 \
                or self.redis_client.scard(self.fetching_key) > 0:
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

                    # self.redis_client.srem(self.fetching_key, *params_list)
    
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
                symbols, start_date_dt, end_date_dt, OHLCV_TIMEFRAME, OHLCV_LIMIT, 1
            ),
            self._consume_ohlcvs_redis(update)
        )
