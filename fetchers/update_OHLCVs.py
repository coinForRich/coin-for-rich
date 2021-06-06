import asyncio
import psycopg2
import datetime
import httpx
from asyncio_throttle import Throttler
from fetchers.config.constants import *
from fetchers.bittrex_fetchOHLCV import bittrex_fetchOHLCV_symbol_OnDemand
from fetchers.bitfinex_fetchOHLCV import bitfinex_fetchOHLCV_symbol_OnDemand


QUERY_LASTEST = '''
select ohlcvss.exchange, symexch.symbol, ohlcvss.time
from symbol_exchange symexch,
   lateral (
      select time, exchange, base_id, quote_id
      from ohlcvs
      where ohlcvs.exchange = symexch.exchange
         and base_id = symexch.base_id
         and quote_id = symexch.quote_id
      order by base_id, quote_id, time desc
      limit 1
   ) ohlcvss;
'''

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
    cur.execute(QUERY_LASTEST)
    results = cur.fetchall()
    if not results:
        print("Error: no results found from ohlcvs table")
    else:
        print(f"Found latest data of {len(results)} rows from ohlcvs table")
        for result in results:
            if result[0] == "bittrex":
                tasks.append(loop.create_task(bittrex_fetchOHLCV_symbol_OnDemand(
                    symbol=result[1],
                    start_date_dt=result[2],
                    end_date_dt=end_date_dt,
                    client=httpx_aclients['bittrex'],
                    conn=conn,
                    throttler=throttlers['bittrex']
                )))
            elif result[0] == "bitfinex":
                tasks.append(loop.create_task(bitfinex_fetchOHLCV_symbol_OnDemand(
                    symbol=result[1],
                    start_date_dt=result[2],
                    end_date_dt=end_date_dt,
                    client=httpx_aclients['bitfinex'],
                    conn=conn,
                    throttler=throttlers['bitfinex']
                )))
        await asyncio.wait(tasks)
    conn.close()

def run_get_and_fetch_all():
    end_date = datetime.datetime.now()
    asyncio.run(get_and_fetch_all(end_date))


if __name__ == "__main__":
    print("Nothing to see")
