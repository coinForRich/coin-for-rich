# This module contains settings for Celery app

from common.config.constants import CELERY_REDIS_URL

# Broker settings, trying Redis
broker_url = CELERY_REDIS_URL

# List of modules to import when the Celery worker starts
include = ['celery_app.celery_tasks']

# Using the database to store task state
result_backend = CELERY_REDIS_URL

# Routing
task_routes = {
    'celery_app.celery_tasks.bitfinex_fetch_ohlcvs_all_symbols': {
        'queue': 'bitfinex_rest'
    },
    'celery_app.celery_tasks.bitfinex_fetch_ohlcvs_symbols': {
        'queue': 'bitfinex_rest'
    },
    'celery_app.celery_tasks.bitfinex_resume_fetch': {
        'queue': 'bitfinex_rest'
    },
    'celery_app.celery_tasks.bitfinex_fetch_ohlcvs_mutual_basequote': {
        'queue': 'bitfinex_rest'
    },
    'celery_app.celery_tasks.binance_fetch_ohlcvs_all_symbols': {
        'queue': 'binance_rest'
    },
    'celery_app.celery_tasks.binance_fetch_ohlcvs_symbols': {
        'queue': 'binance_rest'
    },
    'celery_app.celery_tasks.binance_resume_fetch': {
        'queue': 'binance_rest'
    },
    'celery_app.celery_tasks.binance_fetch_ohlcvs_mutual_basequote': {
        'queue': 'binance_rest'
    },
    'celery_app.celery_tasks.bittrex_fetch_ohlcvs_all_symbols': {
        'queue': 'bittrex_rest'
    },
    'celery_app.celery_tasks.bittrex_fetch_ohlcvs_symbols': {
        'queue': 'bittrex_rest'
    },
    'celery_app.celery_tasks.bittrex_resume_fetch': {
        'queue': 'bittrex_rest'
    },
    'celery_app.celery_tasks.bittrex_fetch_ohlcvs_mutual_basequote': {
        'queue': 'bittrex_rest'
    },
    'celery_app.celery_tasks.ohlcv_websocket_update': {
        'queue': 'updater'
    }
}