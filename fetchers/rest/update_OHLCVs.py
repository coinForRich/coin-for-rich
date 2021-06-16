import asyncio
import psycopg2
import datetime
import httpx
import redis
from asyncio_throttle import Throttler
from fetchers.config.constants import *
from fetchers.config.queries import LATEST_SYMEXCH_QUERY, TS_GAPS_QUERY
from common.helpers.dbhelpers import redis_pipe_sadd
from fetchers.rest.bittrex_fetchOHLCV import bittrex_fetchOHLCV_symbol_OnDemand
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher


def gen_throttlers():
    return {
        'bittrex': Throttler(
                        rate_limit=THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN']['bittrex'], period=THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']
                    ),
        'bitfinex': Throttler(
                        rate_limit=THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN']['bitfinex'], period=THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN']
                    )
    }

def gen_httpx_aclients():
    limits = httpx.Limits(max_connections=HTTPX_MAX_CONCURRENT_CONNECTIONS)
    return {
        'bittrex': httpx.AsyncClient(timeout=None, limits=limits),
        'bitfinex': httpx.AsyncClient(timeout=None, limits=limits)
    }

async def get_and_fetch_all(end_date_dt):
    '''
    get the latest symbols fetched and fetch new data
    from APIs
    params:
        `end_date_dt`: datetime obj
    '''

    throttlers = gen_throttlers()
    httpx_aclients = gen_httpx_aclients()

    loop = asyncio.get_event_loop()
    tasks = []
    conn = psycopg2.connect(DBCONNECTION)
    cur = conn.cursor()
    cur.execute(LATEST_SYMEXCH_QUERY)
    results = cur.fetchall()

    #TODO: rewrite this function using Redis queue

    # if not results:
    #     print("Error: no results found from ohlcvs table")
    # else:
    #     print(f"Found latest data of {len(results)} rows from ohlcvs table")
    #     for result in results:
    #         if result[0] == "bittrex":
    #             tasks.append(loop.create_task(bittrex_fetchOHLCV_symbol_OnDemand(
    #                 symbol=result[1],
    #                 start_date_dt=result[2],
    #                 end_date_dt=end_date_dt,
    #                 client=httpx_aclients['bittrex'],
    #                 conn=conn,
    #                 throttler=throttlers['bittrex']
    #             )))
    #         elif result[0] == "bitfinex":
    #             tasks.append(loop.create_task(bitfinex_fetchOHLCV_symbol_OnDemand(
    #                 symbol=result[1],
    #                 start_date_dt=result[2],
    #                 end_date_dt=end_date_dt,
    #                 client=httpx_aclients['bitfinex'],
    #                 conn=conn,
    #                 throttler=throttlers['bitfinex']
    #             )))
    #     await asyncio.wait(tasks)
    print(f'Fetching completed: symbol data until {end_date_dt}')
    conn.close()

def run_get_and_fetch_all():
    end_date = datetime.datetime.now()
    asyncio.run(get_and_fetch_all(end_date))

async def fillgaps_all():
    with psycopg2.connect(DBCONNECTION) as conn:
        cur = conn.cursor()
        redis_client = redis.Redis(host=REDIS_HOST, decode_responses=True)
        results_empty = False
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
            
def run_fillgaps_all():
    # loop = asyncio.get_event_loop()
    # tasks = []
    asyncio.run(fillgaps_all())


if __name__ == "__main__":
    # print("Nothing to see")
    run_fillgaps_all()
