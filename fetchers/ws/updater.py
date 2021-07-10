# This module collects websocket sub data in Redis, from all exchanges
#   and inserts them into PSQL database

import redis
import time
import psycopg2
from common.helpers.datetimehelpers import milliseconds_to_datetime
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, REDIS_DELIMITER, DBCONNECTION, OHLCVS_TABLE
)
from fetchers.config.constants import (
    WS_SUB_LIST_REDIS_KEY, WS_SUB_PREFIX, WS_SUB_PROCESSING_REDIS_KEY
)
from fetchers.config.queries import PSQL_INSERT_IGNOREDUP_QUERY
from fetchers.helpers.dbhelpers import psql_bulk_insert


UPDATE_FREQUENCY_SECS = 10

class OHLCVWebsocketUpdater:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )

    def update(self):
        '''
        Collects ohlcv data in Redis sub keys and inserts them
            into PSQL db every `UPDATE_FREQUENCY_SECS` seconds
        '''
        psql_conn = psycopg2.connect(DBCONNECTION)
        try:
            while True:
                ohlcvs_table_insert = []
                for key in self.redis_client.smembers(WS_SUB_LIST_REDIS_KEY):
                    try:
                        exchange, base_id, quote_id = \
                            key.split(WS_SUB_PREFIX)[1].split(REDIS_DELIMITER)
                        data = self.redis_client.hgetall(key)
                        # If there is data and len data > 1,
                        #   sort timestamps ascending,  exclude the latest one,
                        #   make ohlcv rows to insert
                        #   then delete data related to inserted timestamps
                        if data and len(data.keys()) > 1:
                            ts_sorted = sorted(data.keys())
                            ts_to_insert = ts_sorted[:-1]
                            for ts in ts_to_insert:
                                ohlcv = data[ts].split(REDIS_DELIMITER)
                                ohlcvs_table_insert.append(
                                    (
                                        milliseconds_to_datetime(int(ts)),
                                        exchange,
                                        base_id,
                                        quote_id,
                                        float(ohlcv[1]),
                                        float(ohlcv[2]),
                                        float(ohlcv[3]),
                                        float(ohlcv[4]),
                                        float(ohlcv[5])
                                    )
                                )
                                # Add processing ohlcv to a Redis set
                                #   in case of crash before data entering PSQL db
                                processing_val = f'{exchange}{REDIS_DELIMITER}{base_id}{REDIS_DELIMITER}{quote_id}{REDIS_DELIMITER}{data[ts]}'
                                self.redis_client.sadd(
                                    WS_SUB_PROCESSING_REDIS_KEY,
                                    processing_val
                                )
                            self.redis_client.hdel(key, *ts_to_insert)
                    except Exception as exc:
                        print(f"EXCEPTION: {exc}")
                psql_bulk_insert(
                    psql_conn,
                    ohlcvs_table_insert,
                    OHLCVS_TABLE,
                    PSQL_INSERT_IGNOREDUP_QUERY
                )
                self.redis_client.delete(WS_SUB_PROCESSING_REDIS_KEY)
                time.sleep(UPDATE_FREQUENCY_SECS)
        finally:
            psql_conn.close()