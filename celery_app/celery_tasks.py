# This module contains Celery tasks

import json
import datetime
from celery_app.celery_main import app
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher
from fetchers.rest.bittrex import BittrexOHLCVFetcher
from fetchers.rest.binance import BinanceOHLCVFetcher
from fetchers.ws.updater import OHLCVWebsocketUpdater
from fetchers.rest.updater import *
from common.helpers.datetimehelpers import str_to_datetime


# Bitfinex
@app.task
def bitfinex_fetch_ohlcvs_all_symbols(start_date, end_date):
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.run_fetch_ohlcvs_all(start_date, end_date)
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_fetch_ohlcvs_symbols(symbols, start_date, end_date):
    '''
    fetches ohlcvs from Bitfinex for a list of symbols
    params (all params are str because Celery serializes args):
        `symbols`: list of symbols
        `start_date`: datetime
        `end_date`: datetime
    '''

    # Symbols need to be de-serialized
    if isinstance(symbols, str):
        symbols = json.loads(symbols)
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.fetch_symbol_data()
    bitfinex_fetcher.run_fetch_ohlcvs(symbols, start_date, end_date)
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_resume_fetch():
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.run_resume_fetch()
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_fetch_ohlcvs_mutual_basequote(start_date, end_date):
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    print(f"Celery: Fetching OHLCVs from {start_date} to {end_date}")
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.fetch_symbol_data()
    bitfinex_fetcher.run_fetch_ohlcvs_mutual_basequote(start_date, end_date)
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_fetch_ohlcvs_mutual_basequote_1hour():
    '''
    Fetches OHLCVs on Bitfinex of mutual symbols for the last 1 hour
    '''

    now = datetime.datetime.now()
    last_min = now - datetime.timedelta(hours=1)
    print(f"Celery: Fetching OHLCVs from {last_min} to {now}")
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.fetch_symbol_data()
    bitfinex_fetcher.run_fetch_ohlcvs_mutual_basequote(last_min, now)
    bitfinex_fetcher.close_connections()

# Bittrex
@app.task
def bittrex_fetch_ohlcvs_all_symbols(start_date, end_date):
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bittrex_fetcher = BittrexOHLCVFetcher()
    bittrex_fetcher.fetch_symbol_data()
    bittrex_fetcher.run_fetch_ohlcvs_all(start_date, end_date)
    bittrex_fetcher.close_connections()

@app.task
def bittrex_fetch_ohlcvs_symbols(symbols, start_date, end_date):
    '''
    fetches ohlcvs from Bittrex for a list of symbols
    params (all params are str because Celery serializes args):
        `symbols`: list of symbols
        `start_date`: datetime
        `end_date`: datetime
    '''

    # Symbols need to be de-serialized
    if isinstance(symbols, str):
        symbols = json.loads(symbols)
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bittrex_fetcher = BittrexOHLCVFetcher()
    bittrex_fetcher.fetch_symbol_data()
    bittrex_fetcher.run_fetch_ohlcvs(symbols, start_date, end_date)
    bittrex_fetcher.close_connections()

@app.task
def bittrex_resume_fetch():
    bittrex_fetcher = BittrexOHLCVFetcher()
    bittrex_fetcher.run_resume_fetch()
    bittrex_fetcher.close_connections()

@app.task
def bittrex_fetch_ohlcvs_mutual_basequote(start_date, end_date):
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bittrex_fetcher = BittrexOHLCVFetcher()
    bittrex_fetcher.fetch_symbol_data()
    bittrex_fetcher.run_fetch_ohlcvs_mutual_basequote(start_date, end_date)
    bittrex_fetcher.close_connections()

# Binance
@app.task
def binance_fetch_ohlcvs_all_symbols(start_date, end_date):
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    binance_fetcher = BinanceOHLCVFetcher()
    binance_fetcher.run_fetch_ohlcvs_all(start_date, end_date)
    binance_fetcher.close_connections()

@app.task
def binance_fetch_ohlcvs_symbols(symbols, start_date, end_date):
    '''
    fetches ohlcvs from Bitfinex for a list of symbols
    params (all params are str because Celery serializes args):
        `symbols`: list of symbols
        `start_date`: datetime
        `end_date`: datetime
    '''

    # Symbols need to be de-serialized
    if isinstance(symbols, str):
        symbols = json.loads(symbols)
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    binance_fetcher = BinanceOHLCVFetcher()
    binance_fetcher.fetch_symbol_data()
    binance_fetcher.run_fetch_ohlcvs(symbols, start_date, end_date)
    binance_fetcher.close_connections()

@app.task
def binance_resume_fetch():
    binance_fetcher = BinanceOHLCVFetcher()
    binance_fetcher.run_resume_fetch()
    binance_fetcher.close_connections()

@app.task
def binance_fetch_ohlcvs_mutual_basequote(start_date, end_date):
    # The dates need to be de-serialized
    # start_date_dt = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    # end_date_dt = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    binance_fetcher = BinanceOHLCVFetcher()
    binance_fetcher.fetch_symbol_data()
    binance_fetcher.run_fetch_ohlcvs_mutual_basequote(start_date, end_date)
    binance_fetcher.close_connections()

# OHLCV update data to psql db from websocket
@app.task
def ohlcv_websocket_update():
    updater = OHLCVWebsocketUpdater()
    updater.update()

# @app.task
# def get_and_fetch_all_task():
#     update_OHLCVs.run_get_and_fetch_all()
