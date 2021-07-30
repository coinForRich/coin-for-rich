# Testing ground

import datetime
import redis
import time
from common.config.constants import *
from fetchers.config.constants import *
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher
from fetchers.rest.bittrex import BittrexOHLCVFetcher
from fetchers.rest.binance import BinanceOHLCVFetcher
from celery_app.celery_tasks import *


start_date_dt=datetime.datetime(2021, 7, 28, 0, 0, 0)
end_date_dt=datetime.datetime(2021, 7, 29, 0, 0, 0)

def run_tasks():
    # bitfinex_fetch_ohlcvs_all_symbols.delay(
    #     start_date = start_date_dt,
    #     end_date = end_date_dt
    # )
    # bitfinex_fetch_ohlcvs_mutual_basequote.delay(
    #     start_date = start_date_dt,
    #     end_date = end_date_dt
    # )

    # binance_fetch_ohlcvs_all_symbols.delay(
    #     start_date = start_date_dt,
    #     end_date = end_date_dt
    # )
    # binance_fetch_ohlcvs_mutual_basequote.delay(
    #     start_date = start_date_dt,
    #     end_date = end_date_dt
    # )

    # bittrex_fetch_ohlcvs_all_symbols.delay(
    #     start_date = start_date_dt,
    #     end_date = end_date_dt
    # )
    # bittrex_fetch_ohlcvs_mutual_basequote.delay(
    #     start_date = start_date_dt,
    #     end_date = end_date_dt
    # )
    pass


if __name__ == "__main__":
    run_tasks()