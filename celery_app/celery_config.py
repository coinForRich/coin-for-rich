# This module contains settings for Celery app

from common.config.constants import CELERY_REDIS_URL

# Broker settings, trying Redis
# broker_url = "amqp://coin_user:yummyicecream@localhost:5672/coin_fetcher"
broker_url = CELERY_REDIS_URL

# List of modules to import when the Celery worker starts
include = ['celery_app.celery_tasks']

# Using the database to store task state
result_backend = CELERY_REDIS_URL

# Routing
task_routes = {
    # 'celery_tasks.bittrex_fetchOHLCV_task': {'queue': 'bittrex'},
    'celery_app.celery_tasks.bitfinex_fetch_ohlcvs_all_symbols': {'queue': 'bitfinex'}
}