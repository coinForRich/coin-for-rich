import asyncio
import psycopg2
import datetime
import httpx
import redis
from asyncio_throttle import Throttler
from common.helpers.dbhelpers import redis_pipe_sadd
from common.config.constants import DBCONNECTION, REDIS_HOST
from fetchers.config.constants import *
from fetchers.config.queries import LATEST_SYMEXCH_QUERY, TS_GAPS_QUERY
from fetchers.rest.bittrex import BittrexOHLCVFetcher
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher


def get_and_fetch_all(end_date_dt):
    '''
    get the latest symbols fetched and fetch new data
    from APIs
    params:
        `end_date_dt`: datetime obj
    '''
    
    with psycopg2.connect(DBCONNECTION) as psql_conn:
        cur = psql_conn.cursor()
        cur.execute(LATEST_SYMEXCH_QUERY)

        bitfinex_fetcher = BitfinexOHLCVFetcher()
        bittrex_fetcher = BittrexOHLCVFetcher()

def fillgaps_all():
    with psycopg2.connect(DBCONNECTION) as psql_conn:
        cur = psql_conn.cursor()
        redis_client = redis.Redis(host=REDIS_HOST, decode_responses=True)
        # while not results_empty:
        #     cur.execute(TS_GAPS_QUERY)
        #     results = cur.fetchall()
        #     if results:
        #         print("Found results from PSQL")
        #         # loop over results and push into Redis queue
        #         push_values = []
        #         for result in results:
        #             start_date = result[0]['time']
        #             end_date = result[0]['next_time']
        #             exchange=result[0]['exchange']
        #             symbol=result[0]['symbol']
        #             push_values.append(
        #                 f'{exchange}{REDIS_DELIMITER}{symbol}{REDIS_DELIMITER}{start_date}{REDIS_DELIMITER}{end_date}'
        #             )
        #         if exchange == "bittrex":
        #             redis_pipe_sadd(redis_client, OHLCVS_BITTREX_REDIS_KEY, push_values)
        #             print(f'Length of bittrex Redis key: {redis_client.llen(OHLCVS_BITTREX_REDIS_KEY)}')
        #         elif exchange == "bitfinex":
        #             redis_pipe_sadd(redis_client, OHLCVS_BITFINEX_REDIS_KEY, push_values)
        #             print(f'Length of bitfinex Redis key: {redis_client.llen(OHLCVS_BITFINEX_REDIS_KEY)}')
        #     else:
        #         results_empty = True
        #         break
        #     await asyncio.sleep(0.25)
            

if __name__ == "__main__":
    print("Nada")
    # run_fillgaps_all()
