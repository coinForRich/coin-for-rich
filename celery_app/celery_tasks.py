# This module contains Celery tasks

import json
import datetime
from celery_app.celery_main import app
from common.helpers.datetimehelpers import str_to_datetime
from fetchers.rest.bitfinex import BitfinexOHLCVFetcher
from fetchers.rest.bittrex import BittrexOHLCVFetcher
from fetchers.rest.binance import BinanceOHLCVFetcher


# Fetch symbol data to get all symbols into
#   symbol_exchange psql table
@app.task
def all_fetch_symbol_data():
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    binance_fetcher = BinanceOHLCVFetcher()
    bittrex_fetcher = BittrexOHLCVFetcher()
    bitfinex_fetcher.fetch_symbol_data()
    binance_fetcher.fetch_symbol_data()
    bittrex_fetcher.fetch_symbol_data()
    bitfinex_fetcher.close_connections()
    binance_fetcher.close_connections()
    bittrex_fetcher.close_connections()

# Bitfinex
@app.task
def bitfinex_fetch_ohlcvs_all_symbols(start_date, end_date):
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bitfinex_fetcher.run_fetch_ohlcvs_all(start_date, end_date)
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_fetch_ohlcvs_symbols(symbols, start_date, end_date):
    '''
    fetches ohlcvs from Bitfinex for a list of symbols
    params (all params are str because Celery serializes args):
        `symbols`: list of symbols
        `start_date`: string of datetime
        `end_date`: string of datetime
    '''

    bitfinex_fetcher = BitfinexOHLCVFetcher()
    # Symbols need to be de-serialized
    if isinstance(symbols, str):
        symbols = json.loads(symbols)
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bitfinex_fetcher.run_fetch_ohlcvs(symbols, start_date, end_date)
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_resume_fetch():
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    bitfinex_fetcher.run_resume_fetch()
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_fetch_ohlcvs_mutual_basequote(start_date, end_date):
    bitfinex_fetcher = BitfinexOHLCVFetcher()
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    print(f"Celery: Fetching OHLCVs from {start_date} to {end_date}")
    bitfinex_fetcher.run_fetch_ohlcvs_mutual_basequote(start_date, end_date)
    bitfinex_fetcher.close_connections()

@app.task
def bitfinex_fetch_ohlcvs_mutual_basequote_1min():
    '''
    Periodically fetches OHLCVs on Bitfinex of mutual symbols
        from 4 minutes before to 1 minute before
    '''

    bitfinex_fetcher = BitfinexOHLCVFetcher()
    end = datetime.datetime.now() - datetime.timedelta(minutes=1)
    start = end - datetime.timedelta(minutes=4)
    print(f"Celery: Fetching OHLCVs from {start} to {end}")
    bitfinex_fetcher.run_fetch_ohlcvs_mutual_basequote(start, end, update=True)

# Binance
@app.task
def binance_fetch_ohlcvs_all_symbols(start_date, end_date):
    binance_fetcher = BinanceOHLCVFetcher()
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
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

    binance_fetcher = BinanceOHLCVFetcher()
    # Symbols need to be de-serialized
    if isinstance(symbols, str):
        symbols = json.loads(symbols)
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    binance_fetcher.run_fetch_ohlcvs(symbols, start_date, end_date)
    binance_fetcher.close_connections()

@app.task
def binance_resume_fetch():
    binance_fetcher = BinanceOHLCVFetcher()
    binance_fetcher.run_resume_fetch()
    binance_fetcher.close_connections()

@app.task
def binance_fetch_ohlcvs_mutual_basequote(start_date, end_date):
    binance_fetcher = BinanceOHLCVFetcher()
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    binance_fetcher.run_fetch_ohlcvs_mutual_basequote(start_date, end_date)
    binance_fetcher.close_connections()

@app.task
def binance_fetch_ohlcvs_mutual_basequote_1min():
    '''
    Periodically fetches OHLCVs on Binance of mutual symbols
        from 4 minutes before to 1 minute before
    '''

    binance_fetcher = BinanceOHLCVFetcher()
    end = datetime.datetime.now() - datetime.timedelta(minutes=1)
    start = end - datetime.timedelta(minutes=4)
    print(f"Celery: Fetching OHLCVs from {start} to {end}")
    binance_fetcher.run_fetch_ohlcvs_mutual_basequote(start, end, update=True)

# Bittrex
@app.task
def bittrex_fetch_ohlcvs_all_symbols(start_date, end_date):
    bittrex_fetcher = BittrexOHLCVFetcher()
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
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

    bittrex_fetcher = BittrexOHLCVFetcher()
    # Symbols need to be de-serialized
    if isinstance(symbols, str):
        symbols = json.loads(symbols)
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bittrex_fetcher.run_fetch_ohlcvs(symbols, start_date, end_date)
    bittrex_fetcher.close_connections()

@app.task
def bittrex_resume_fetch():
    bittrex_fetcher = BittrexOHLCVFetcher()
    bittrex_fetcher.run_resume_fetch()
    bittrex_fetcher.close_connections()

@app.task
def bittrex_fetch_ohlcvs_mutual_basequote(start_date, end_date):
    bittrex_fetcher = BittrexOHLCVFetcher()
    # The dates need to be de-serialized
    start_date = str_to_datetime(start_date, f='%Y-%m-%dT%H:%M:%S')
    end_date = str_to_datetime(end_date, f='%Y-%m-%dT%H:%M:%S')
    bittrex_fetcher.run_fetch_ohlcvs_mutual_basequote(start_date, end_date)
    bittrex_fetcher.close_connections()

@app.task
def bittrex_fetch_ohlcvs_mutual_basequote_1min():
    '''
    Periodically fetches OHLCVs on Bittrex of mutual symbols
        from 4 minutes before to 1 minute before
    '''

    bittrex_fetcher = BittrexOHLCVFetcher()
    end = datetime.datetime.now() - datetime.timedelta(minutes=1)
    start = end - datetime.timedelta(minutes=4)
    print(f"Celery: Fetching OHLCVs from {start} to {end}")
    bittrex_fetcher.run_fetch_ohlcvs_mutual_basequote(start, end, update=True)
