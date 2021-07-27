# # Testing ground

# import datetime
# import redis
# import time
# from common.config.constants import *
# from fetchers.config.constants import *
# from fetchers.rest.bitfinex import BitfinexOHLCVFetcher
# from fetchers.rest.bittrex import BittrexOHLCVFetcher
# from fetchers.rest.binance import BinanceOHLCVFetcher
# from celery_app.celery_tasks import *


# start_date_dt=datetime.datetime(2020, 1, 1, 0, 0, 0)
# end_date_dt=datetime.datetime(2021, 7, 18, 0, 0, 0)

# def run_tasks():
#     # bitfinex_fetch_ohlcvs_all_symbols.delay(
#     #     start_date = start_date_dt,
#     #     end_date = end_date_dt
#     # )
#     # bitfinex_resume_fetch.delay()
#     # bitfinex_fetcher = BitfinexOHLCVFetcher()
#     # bitfinex_fetcher.run_fetch_ohlcvs_all(start_date_dt, end_date_dt)
#     # bitfinex_fetcher.close_connections()
#     # bitfinex_fetch_ohlcvs_mutual_basequote.delay(
#     #     start_date = start_date_dt,
#     #     end_date = end_date_dt
#     # )

#     # binance_fetcher = BinanceOHLCVFetcher()
#     # binance_fetcher.run_fetch_ohlcvs_all(start_date_dt, end_date_dt)
#     # binance_fetcher.close_connections()
#     # binance_fetch_ohlcvs_all_symbols.delay(
#     #     start_date = start_date_dt,
#     #     end_date = end_date_dt
#     # )
#     # binance_fetch_ohlcvs_mutual_basequote.delay(
#     #     start_date = start_date_dt,
#     #     end_date = end_date_dt
#     # )

#     # bittrex_fetch_ohlcvs_all_symbols.delay(
#     #     start_date = start_date_dt,
#     #     end_date = end_date_dt
#     # )
#     # bittrex_fetch_ohlcvs_mutual_basequote.delay(
#     #     start_date = start_date_dt,
#     #     end_date = end_date_dt
#     # )

#     # ohlcv_websocket_update.delay()
#     pass


# if __name__ == "__main__":
#     run_tasks()