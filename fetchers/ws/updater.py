# This module collects websocket subbed data in Redis, from all exchanges
#   and inserts them into PSQL database

import redis
import time
import psycopg2
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, REDIS_DELIMITER, DBCONNECTION, OHLCVS_TABLE
)
from common.utils.logutils import create_logger
from common.helpers.datetimehelpers import milliseconds_to_datetime
from common.helpers.numbers import round_decimal
from fetchers.config.constants import (
    WS_SUB_LIST_REDIS_KEY, WS_SUB_PREFIX, WS_SUB_PROCESSING_REDIS_KEY
)
from fetchers.config.queries import PSQL_INSERT_IGNOREDUP_QUERY
from fetchers.helpers.dbhelpers import psql_bulk_insert


UPDATE_FREQUENCY_SECS = 15

class OHLCVWebsocketUpdater:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )

        self.logger = create_logger("updater_websocket")
        self.psql_conn = psycopg2.connect(DBCONNECTION)

    def update(self):
        '''
        Collects ohlcv data in ws sub Redis keys and inserts them
            into PSQL db every `UPDATE_FREQUENCY_SECS` seconds
        '''

        try:
            while True:
                self.logger.info("Collecting subscribed OHLCV data in Redis")
                ohlcvs_table_insert = []
                # Loop thru all keys in sub list
                self.logger.info(
                    f"Length of WS sub list: {self.redis_client.scard(WS_SUB_LIST_REDIS_KEY)}")
                
                # TODO: the sub list needs to be cleaned for stale values
                for key in self.redis_client.smembers(WS_SUB_LIST_REDIS_KEY):
                    try:
                        exchange, base_id, quote_id = \
                            key.split(WS_SUB_PREFIX)[1].split(REDIS_DELIMITER)
                        data = self.redis_client.hgetall(key)
                        # If there is data and len data > 1,
                        #   sort timestamps ascending,  exclude the latest one,
                        #   prepare ohlcv rows to insert
                        #   then delete data related to inserted timestamps
                        if data and len(data) > 1:
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
                                        round_decimal(ohlcv[1]),
                                        round_decimal(ohlcv[2]),
                                        round_decimal(ohlcv[3]),
                                        round_decimal(ohlcv[4]),
                                        round_decimal(ohlcv[5])
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
                        self.logger.warning(f"EXCEPTION: {exc}")
                        raise exc
                # Bulk insert
                try:
                    success = psql_bulk_insert(
                        self.psql_conn,
                        ohlcvs_table_insert,
                        OHLCVS_TABLE,
                        insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
                    )
                    # If success, clean up
                    # TODO: add a process to clean up if NOT success
                    if success:
                        self.redis_client.delete(WS_SUB_PROCESSING_REDIS_KEY)
                        self.logger.info("Updated OHLCV to PSQL db")
                except psycopg2.InterfaceError as exc:
                    # Reconnects if connection is closed
                    self.logger.warning(f"EXCEPTION: {exc}. Reconnecting...")
                    self.psql_conn = psycopg2.connect(DBCONNECTION)
                except Exception as exc:
                    self.logger.warning(f"EXCEPTION: {exc}")
                    raise exc
                time.sleep(UPDATE_FREQUENCY_SECS)
        finally:
            self.psql_conn.close()


# if __name__ == "__main__":
#     run_cmd = sys.argv[1]
#     updater = OHLCVWebsocketUpdater()
#     if getattr(updater, run_cmd):
#         getattr(updater, run_cmd)()
