# This module contains Celery tasks

from celery_app.celery_main import app
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher
from fetchers.rest import bittrex_fetchOHLCV
from fetchers.rest import update_OHLCVs
from fetchers.helpers.datetimehelpers import str_to_datetime


@app.task
def bitfinex_fetch_ohlcvs_all_symbols(start_date, end_date):
    # The dates need to be de-serialized
    start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')

    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.run_fetch_ohlcvs_all(start_date_dt, end_date_dt)
    bitfinex_fetcher.close_connections()

# @app.task
# def bitfinex_fetchOHLCV_OnDemand_task(symbol, start_date, end_date):
#     # The dates need to be de-serialized
#     start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
#     end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
#     bitfinex_fetchOHLCV.run_OnDemand(symbol, start_date_dt, end_date_dt)

# @app.task
# def bittrex_fetchOHLCV_OnDemand_task(symbol, start_date, end_date):
#     # The dates need to be de-serialized
#     start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
#     end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
#     bittrex_fetchOHLCV.run_OnDemand(symbol, start_date_dt, end_date_dt)

# @app.task
# def get_and_fetch_all_task():
#     update_OHLCVs.run_get_and_fetch_all()
