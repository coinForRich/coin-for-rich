# This module collects websocket subbed data in Redis, from all exchanges
#   and inserts them into PSQL database

import time
import redis
import psycopg2
from typing import NoReturn
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, REDIS_DELIMITER, DBCONNECTION, OHLCVS_TABLE
)
from common.utils.logutils import create_logger
from common.helpers.datetimehelpers import (
    milliseconds_to_datetime, redis_time, milliseconds
)
from common.helpers.numbers import round_decimal
from fetchers.config.constants import (
    WS_SUB_LIST_REDIS_KEY, WS_SUB_PREFIX,
    WS_SUB_PROCESSING_REDIS_KEY, NUM_DECIMALS
)
from fetchers.config.queries import PSQL_INSERT_IGNOREDUP_QUERY
from fetchers.helpers.dbhelpers import psql_bulk_insert
from fetchers.helpers.ws import (
    make_sub_val, make_sub_redis_key, make_send_redis_key
)


UPDATE_FREQUENCY_SECS = 10
DATA_HELD_MLS_THRESHOLD = 3600000 # 1 day

class OHLCVWebsocketUpdater:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            username="default",
            password=REDIS_PASSWORD,
            decode_responses=True
        )

        self.logger = create_logger("WS_fetcher_updater")
        self.psql_conn = psycopg2.connect(DBCONNECTION)

    @classmethod
    def make_rows_insert(
            cls, ts: str, exch: str, base: str, quote: str, ohlcv: list
        ) -> tuple:
        '''
        Makes rows to insert to PSQL db

        :params:
            `ohlcv`: list of unpacked OHLCV
        '''

        return (
            milliseconds_to_datetime(int(ts)),
            exch,
            base,
            quote,
            round_decimal(ohlcv[1], NUM_DECIMALS),
            round_decimal(ohlcv[2], NUM_DECIMALS),
            round_decimal(ohlcv[3], NUM_DECIMALS),
            round_decimal(ohlcv[4], NUM_DECIMALS),
            round_decimal(ohlcv[5], NUM_DECIMALS)
        )

    @classmethod
    def make_processing_val(
            cls, exch: str, base: str, quote: str, ohlcv_str: str
        ) -> str:
        '''
        Makes a serialized processing value from WS sub list

        This serialized value is a temp backup in case
            of a crash before actual OHLCV data enter PSQL db

        :params:
            `ohlcv_str`: string of packed OHLCV;
                something like `{t}{d}{o}{d}{h}{d}{l}{d}{c}{d}{v}`
                see: `fetchers.helpers.ws` module
        '''

        return f'{exch}{REDIS_DELIMITER}{base}{REDIS_DELIMITER}{quote}{REDIS_DELIMITER}{ohlcv_str}'

    def prepare_insert(
            self, data: dict, insert_list: list,
            ts: str, exch: str, base: str, quote: str
        ) -> None:
        '''
            Prepares rows for bulk insert

            :params:
                `data`: dict of OHLCV from WS sub list
                `insert_list`: list of OHLCV rows to insert
        '''

        ohlcv = data[ts].split(REDIS_DELIMITER)
        insert_list.append(
            self.make_rows_insert(
                ts, exch, base, quote, ohlcv
            )
        )
        # Add processing ohlcv to a Redis set
        #   in case of crash before data entering PSQL db
        processing_val = self.make_processing_val(
            exch,
            base,
            quote,
            data[ts]
        )
        self.redis_client.sadd(
            WS_SUB_PROCESSING_REDIS_KEY,
            processing_val
        )
    
    def update(self) -> NoReturn:
        '''
        Collects ohlcv data in ws sub Redis keys and inserts them
            into PSQL db every `UPDATE_FREQUENCY_SECS` seconds
        '''

        try:
            while True:
                self.logger.info("Collecting subscribed OHLCV data in Redis")
                self.logger.info(
                    f"Length of WS sub list: {self.redis_client.scard(WS_SUB_LIST_REDIS_KEY)}")
                ohlcvs_table_insert = []
                for key in self.redis_client.smembers(WS_SUB_LIST_REDIS_KEY):
                    exchange, base_id, quote_id = \
                        key.split(WS_SUB_PREFIX)[1].split(REDIS_DELIMITER)
                    data = self.redis_client.hgetall(key)
                    # If no data, remove the key
                    # Elif there is data and len data > 1,
                    #   sort timestamps ascending, exclude the latest one,
                    #   prepare ohlcv rows to insert
                    #   then delete data related to inserted timestamps
                    if not data:
                        self.redis_client.srem(WS_SUB_LIST_REDIS_KEY, key)
                        self.logger.info(
                            f"WS Fetcher Updater: Removed empty key {key} from sub list")
                    elif len(data) == 1:
                        ts = list(data.keys())[0]
                        now = milliseconds(redis_time(self.redis_client))
                        if now - int(ts) > DATA_HELD_MLS_THRESHOLD: # more than 1 hour
                            self.prepare_insert(
                                data, ohlcvs_table_insert, ts,
                                exchange, base_id, quote_id
                            )
                            self.redis_client.srem(WS_SUB_LIST_REDIS_KEY, key)
                            self.redis_client.delete(key)
                            self.logger.info(
                                f"WS Fetcher Updater: Key {key} has been holding data for more than 1 hour - inserting that value to PSQL db and removing key {key}")
                    elif len(data) > 1:
                        ts_to_insert = sorted(data.keys())[:-1]
                        for ts in ts_to_insert:
                            self.prepare_insert(
                                data, ohlcvs_table_insert, ts,
                                exchange, base_id, quote_id
                            )
                        self.redis_client.hdel(key, *ts_to_insert)
                # Bulk insert
                try:
                    success = psql_bulk_insert(
                        self.psql_conn,
                        ohlcvs_table_insert,
                        OHLCVS_TABLE,
                        insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
                    )
                    # If success, clean up
                    # If not, unpack the values and resend them back to
                    #   corresponding `ws_sub_redis_key`
                    # TODO: the unpacking work should be done by a separate worker;
                    #   maybe a cleaner? this looks messy
                    if success:
                        self.logger.info(
                            f"WS Fetcher Updater: Successfully updated OHLCV to PSQL db - {len(ohlcvs_table_insert)} rows")
                        self.redis_client.delete(WS_SUB_PROCESSING_REDIS_KEY)
                    else:
                        self.logger.warning(
                            "WS Fetcher Updater: Failed to update OHLCV to PSQL db - sending OHLCV values back to sub redis key")
                        for processing_val in \
                            self.redis_client.smembers(WS_SUB_PROCESSING_REDIS_KEY):
                            exch, base, quote, \
                            ts, open_, high_, \
                            low_, close_, volume_ \
                                = processing_val.split(REDIS_DELIMITER)
                            sub_val = make_sub_val(
                                ts,
                                open_, high_, low_, close_, volume_,
                                REDIS_DELIMITER
                            )
                            ws_sub_redis_key = make_sub_redis_key(
                                exch,
                                base,
                                quote,
                                REDIS_DELIMITER
                            )
                            self.redis_client.sadd(
                                WS_SUB_LIST_REDIS_KEY, ws_sub_redis_key)
                            self.redis_client.hset(
                                ws_sub_redis_key, ts, sub_val)
                            self.redis_client.srem(
                                WS_SUB_PROCESSING_REDIS_KEY,
                                processing_val
                            )
                # Reconnects if connection is closed
                except psycopg2.InterfaceError as exc:
                    self.logger.warning(
                        f"WS Fetcher Updater: EXCEPTION: {exc}. Reconnecting...")
                    self.psql_conn = psycopg2.connect(DBCONNECTION)
                except Exception as exc:
                    self.logger.error(
                        f"WS Fetcher Updater: EXCEPTION: {exc}")
                    raise exc
                time.sleep(UPDATE_FREQUENCY_SECS)
        finally:
            self.psql_conn.close()
