import redis
import psycopg2
import time
from config.constants import *
from helpers.dbhelpers import *


redis_client = redis.Redis(host=REDIS_HOST, decode_responses=True)
base_id = "BTC"
quote_id = "USD"
exchange = "bitfinex"
psql_conn = psycopg2.connect(DBCONNECTION)
psql_cur = psql_conn.cursor()

query = '''
select row_to_json(t)
    from (
        select "time" as "time", opening_price, highest_price
        lowest_price, closing_price, volume
        from ohlcvs
        where exchange='bitfinex'
        and base_id='BTC'
        and quote_id='USD'
        order by time asc
    ) t;
'''
start = time.time()

psql_cur.execute(query)
results_empty = False
redis_pipe = redis_client.pipeline()
while not results_empty:
    fetch_start = time.time()
    results = psql_cur.fetchmany(PSQL_BATCHSIZE)
    fetch_end = time.time()
    
    print(f'Fetch from sql took {fetch_end - fetch_start} seconds')
    
    if results:
        rpush_start = time.time()
        for result in results:
            # Push key, value in each SQL result row to Redis
            for k,v in result[0].items():
                redis_pipe.rpush(f'{exchange}:{base_id}:{quote_id}:{k}', v)
        redis_pipe.execute()
        rpush_end = time.time()

        print(f'Push to Redis took {rpush_end - rpush_start} seconds')

    else:
        results_empty = True

end = time.time()
elapsed = end - start
print(f'The process took {elapsed} seconds')

'''
With PSQL batch size of 10000, for each batch:
Push to Redis took 1.8706762790679932 seconds
Fetch from sql took 0.0885319709777832 seconds
'''