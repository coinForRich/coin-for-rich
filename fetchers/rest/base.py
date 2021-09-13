# Base for all REST fetchers

import datetime
import psycopg2
import redis
import httpx
import asyncio
from asyncio.events import AbstractEventLoop
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD,
    SYMBOL_EXCHANGE_TABLE, DBCONNECTION
)
from common.utils.asyncioutils import aio_set_exception_handler
from common.utils.logutils import create_logger
from fetchers.config.constants import (
    HTTPX_MAX_CONCURRENT_CONNECTIONS, HTTPX_DEFAULT_TIMEOUT,
    OHLCVS_TOFETCH_REDIS_KEY, OHLCVS_FETCHING_REDIS_KEY,
    SYMEXCH_UNIQUE_COLUMNS, SYMEXCH_UPDATE_COLUMNS
)
from fetchers.config.queries import (
    MUTUAL_BASE_QUOTE_QUERY,
    PSQL_INSERT_IGNOREDUP_QUERY,
    PSQL_INSERT_UPDATE_QUERY
)
from fetchers.helpers.dbhelpers import psql_bulk_insert


class BaseOHLCVFetcher:
    '''
    Base REST fetcher for all exchanges
    '''

    def __init__(self, exchange_name: str):
        # Name, Redis to-fetch and fetching set keys
        self.exchange_name = exchange_name
        self.tofetch_key = OHLCVS_TOFETCH_REDIS_KEY.format(exchange=exchange_name)
        self.fetching_key = OHLCVS_FETCHING_REDIS_KEY.format(exchange=exchange_name)

        # Postgres connection
        # TODO: Not sure if this is needed
        self.psql_conn = psycopg2.connect(DBCONNECTION)
        self.psql_cur = self.psql_conn.cursor()
        
        # Redis client
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )

        # HTTPX limits
        self.httpx_limits = httpx.Limits(
            max_connections=HTTPX_MAX_CONCURRENT_CONNECTIONS[exchange_name]
        )
        self.httpx_timout = httpx.Timeout(HTTPX_DEFAULT_TIMEOUT)

        # Redis initial feeding status
        self.feeding = False

        # Log
        self.logger = create_logger(exchange_name)

        # Symbol data
        self.symbol_data = {}
        

    def _setup_event_loop(self) -> AbstractEventLoop:
        '''
        Gets the event loop or resets it
        '''
        
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
        aio_set_exception_handler(loop)
        return loop

    # TODO: see if this empty method is needed
    async def _fetch_ohlcvs_symbols(*args, **kwargs) -> None:
        '''
        Function to get OHLCVs of symbols
        '''
        
        pass
    
    async def _resume_fetch(self, update: bool=False) -> None:
        '''
        Resumes fetching tasks if there're params inside Redis sets
        '''

        # Asyncio gather 1 task:
        # - Consume from Redis to-fetch
        await asyncio.gather(
            self.consume_ohlcvs_redis(update)
        )
    
    def close_connections(self) -> None:
        '''
        Interface to close all connections (e.g., PSQL)
        '''

        self.psql_conn.close()

    def fetch_symbol_data(self) -> None:
        '''
        Interface to fetch symbol data (exchange, base, quote)
            from self.symbol_data into PSQL db

        Updates is_trading status in PSQL db for existing ones
        '''
        
        rows = [
            (
                self.exchange_name,
                bq['base_id'],
                bq['quote_id'],
                symbol,
                True
            ) for symbol, bq in self.symbol_data.items()
        ]
        psql_bulk_insert(
            self.psql_conn,
            rows,
            SYMBOL_EXCHANGE_TABLE,
            insert_update_query = PSQL_INSERT_UPDATE_QUERY,
            unique_cols = SYMEXCH_UNIQUE_COLUMNS,
            update_cols = SYMEXCH_UPDATE_COLUMNS
        )

    def get_symbols_from_exch(self, query: str) -> dict:
        '''
        Interface to return a dict of symbols from a pre-constructed query
            in this form:
                {
                    'ETHBTC': {
                        'base_id': 'ETH',
                        'quote_id': 'BTC'
                    }
                }
            
        The query must have a `%s` placeholder for the exchange

        Primary use is to get a specific set of symbols
        '''

        self.psql_cur.execute(query, (self.exchange_name,))
        results = self.psql_cur.fetchall()
        ret = {}
        for result in results:
            ret[result[0]] = {
                'base_id': self.symbol_data[result[0]]['base_id'],
                'quote_id': self.symbol_data[result[0]]['quote_id']
            }
        return ret

    def run_fetch_ohlcvs(
        self,
        symbols: list,
        start_date_dt: datetime.datetime,
        end_date_dt: datetime.datetime,
        update: bool=False
    ) -> None:
        '''
        Interface to run fetching OHLCVS for some specified symbols

        :params:
            `symbols`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
            `update`: bool - whether to update when inserting
                to PSQL database
        '''

        loop = self._setup_event_loop()
        try:
            self.logger.info("Run_fetch_ohlcvs: Fetching OHLCVS for indicated symbols")
            loop.run_until_complete(
                self._fetch_ohlcvs_symbols(symbols, start_date_dt, end_date_dt, update)
            )
        finally:
            self.logger.info(
                "Run_fetch_ohlcvs: Finished fetching OHLCVS for indicated symbols")
            loop.close()

    def run_fetch_ohlcvs_all(
        self,
        start_date_dt: datetime.datetime,
        end_date_dt: datetime.datetime,
        update: bool=False
    ) -> None:
        '''
        Interface to run the fetching OHLCVS for all symbols
        
        :params:
            `symbols`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
        '''

        # Have to fetch symbol data first to
        # make sure it's up-to-date
        self.fetch_symbol_data()
        symbols = self.symbol_data.keys()

        self.run_fetch_ohlcvs(symbols, start_date_dt, end_date_dt, update)
        self.logger.info("Run_fetch_ohlcvs_all: Finished fetching OHLCVS for all symbols")

    def run_fetch_ohlcvs_mutual_basequote(
        self,
        start_date_dt: datetime.datetime,
        end_date_dt: datetime.datetime,
        update: bool=False
    ) -> None:
        '''
        Interface to run the fetching of the mutual base-quote symbols
        
        :params:
            `start_date_dt`: datetime obj
            `end_date_dt`: datetime obj
        '''
        # Have to fetch symbol data first to
        # make sure it's up-to-date
        self.fetch_symbol_data()
        
        symbols = self.get_symbols_from_exch(MUTUAL_BASE_QUOTE_QUERY)
        self.run_fetch_ohlcvs(symbols.keys(), start_date_dt, end_date_dt, update)
        self.logger.info(
            "Run_fetch_ohlcvs_mutual_basequote: Finished fetching OHLCVS for mutual symbols"
        )

    def run_resume_fetch(self) -> None:
        '''
        Interface to run the resuming of fetching tasks
        '''

        loop = self._setup_event_loop()
        try:
            self.logger.info("Run_resume_fetch: Resuming fetching tasks from Redis sets")
            loop.run_until_complete(self._resume_fetch())
        finally:
            self.logger.info("Run_resume_fetch: Finished fetching OHLCVS")
            loop.close()
