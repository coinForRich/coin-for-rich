# This module contains db helpers

import csv
import json
from io import StringIO


def psql_copy_from_csv(conn, rows, table, cursor=None):
    '''
    Psycopg2 copy from rows to table using StringIO and CSV
    Do nothing when error occurs
    params:
        `conn`: psycopg2 conn obj (required)
        `rows`: iterable of tuples
        `table`: string - table name
        `cursor`: psycopg2 cursor obj
    '''

    self_cursor = False
    if not cursor:
        self_cursor = True
        cursor = conn.cursor()

    try:
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        buffer.seek(0)
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
        print(f'Rows successfully copied to table {table}')
    except Exception as exc:
        print(f'EXCEPTION: Exception {exc} occurred when copying rows to {table}')
        conn.rollback()
    if self_cursor:
        cursor.close()
    return 1
    
def enqueue_ohlcvs_redis(redis_client, delimiter, key, exchange, symbol, start_date, end_date):
    '''
    enqueue values into Redis to begin fetching OHLCV data
    params:
        `redis_server`: Redis server obj
        `delimiter`: string - delimiter for Redis
        `key`: string, Redis key of the Redis queue
        `exchange`: string
        `symbol`: string
        `start_date`: string resulted from strptime
        `end_date`: string resulted from strptime
    '''
    value = f'{exchange}{delimiter}{symbol}{delimiter}{start_date}{delimiter}{end_date}'
    redis_client.rpush(key, value)
