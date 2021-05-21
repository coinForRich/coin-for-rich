# This module contains Celery tasks


from pathlib import PureWindowsPath
from celery_main import app
from fetchers import bitfinex_fetchOHLCV
from fetchers import bittrex_fetchOHLCV
from fetchers.helpers.datetimehelpers import str_to_datetime


@app.task
def bittrex_fetchOHLCV_task():
    bittrex_fetchOHLCV.run()

@app.task
def bitfinex_fetchOHLCV_task():
    bitfinex_fetchOHLCV.run()

@app.task
def bittrex_fetchOHLCV_OnDemand_task(symbol, start_date, end_date):
    # The dates need to be de-serialized
    start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bittrex_fetchOHLCV.run_OnDemand(symbol, start_date_dt, end_date_dt)
