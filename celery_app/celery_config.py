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
    'celery_app.celery_tasks.bitfinex_fetch_ohlcvs_all_symbols': {'queue': 'bitfinex'},
    'celery_app.celery_tasks.bitfinex_fetch_ohlcvs_symbols': {'queue': 'bitfinex'},
    'celery_app.celery_tasks.bitfinex_resume_fetch': {'queue': 'bitfinex'},
    'celery_app.celery_tasks.binance_fetch_ohlcvs_all_symbols': {'queue': 'binance'},
    'celery_app.celery_tasks.binance_fetch_ohlcvs_symbols': {'queue': 'binance'},
    'celery_app.celery_tasks.binance_resume_fetch': {'queue': 'binance'},
    'celery_app.celery_tasks.bittrex_fetch_ohlcvs_all_symbols': {'queue': 'bittrex'},
    'celery_app.celery_tasks.bittrex_fetch_ohlcvs_symbols': {'queue': 'bittrex'}
}