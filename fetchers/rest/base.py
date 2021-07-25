# Base for all REST fetchers

import datetime
import asyncio
from common.config.constants import REDIS_HOST, REDIS_PASSWORD, SYMBOL_EXCHANGE_TABLE
from common.utils.asyncioutils import aio_set_exception_handler
from fetchers.config.constants import REST_RATE_LIMIT_REDIS_KEY, THROTTLER_RATE_LIMITS
from fetchers.config.queries import MUTUAL_BASE_QUOTE_QUERY, PSQL_INSERT_IGNOREDUP_QUERY
from fetchers.helpers.dbhelpers import psql_bulk_insert


class BaseOHLCVFetcher:
    '''
    Base REST fetcher for all exchanges
    '''

    def __init__(self):
        pass
    
    def close_connections(self):
        '''
        Close all connections (e.g., PSQL)
        '''

        self.psql_conn.close()

    def fetch_symbol_data(self):
        rows = [
            (self.exchange_name, bq['base_id'], bq['quote_id'], symbol) \
            for symbol, bq in self.symbol_data.items()
        ]
        psql_bulk_insert(
            self.psql_conn,
            rows,
            SYMBOL_EXCHANGE_TABLE,
            insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
        )
    
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
        
        self.psql_cur.execute(MUTUAL_BASE_QUOTE_QUERY, (self.exchange_name,))
        results = self.psql_cur.fetchall()
        ret = {}
        for result in results:
            ret[result[0]] = {
                'base_id': self.symbol_data[result[0]]['base_id'],
                'quote_id': self.symbol_data[result[0]]['quote_id']
            }
        return ret

    async def resume_fetch(self, update: bool=False):
        '''
        Resumes fetching tasks if there're params inside Redis sets
        '''

        # Asyncio gather 1 task:
        # - Consume from Redis to-fetch
        await asyncio.gather(
            self.consume_ohlcvs_redis(update)
        )

    def run_fetch_ohlcvs(
        self,
        symbols: list,
        start_date_dt: datetime.datetime,
        end_date_dt: datetime.datetime,
        update: bool=False
    ):
        '''
        Runs fetching OHLCVS

        :params:
            `symbols`: list of symbol string
            `start_date_dt`: datetime obj - for start date
            `end_date_dt`: datetime obj - for end date
            `update`: bool - whether to update when inserting
                to PSQL database
        '''

        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
        aio_set_exception_handler(loop)
        try:
            print("Run_fetch_ohlcvs: Fetching OHLCVS for indicated symbols")
            loop.run_until_complete(
                self.fetch_ohlcvs_symbols(symbols, start_date_dt, end_date_dt, update)
            )
        finally:
            print("Run_fetch_ohlcvs: Finished fetching OHLCVS for indicated symbols")
            loop.close()

    def run_fetch_ohlcvs_all(self, start_date_dt, end_date_dt, update: bool=False):
        '''
        Runs the fetching OHLCVS for all symbols
        
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

    def run_fetch_ohlcvs_mutual_basequote(
        self,
        start_date_dt,
        end_date_dt,
        update: bool=False
    ):
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
        self.run_fetch_ohlcvs(symbols.keys(), start_date_dt, end_date_dt, update)
        print("Run_fetch_ohlcvs_all: Finished fetching OHLCVS for mutual symbols")
