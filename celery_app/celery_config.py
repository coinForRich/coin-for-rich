# This module contains settings for Celery app

from common.config.constants import REDIS_HOST

# Broker settings, trying Redis
# broker_url = "amqp://coin_user:yummyicecream@localhost:5672/coin_fetcher"
broker_url = f'redis://{REDIS_HOST}:6379'

# List of modules to import when the Celery worker starts
include = ['celery_app.celery_tasks']

# Using the database to store task state
result_backend = f'redis://{REDIS_HOST}:6379'

# Routing
task_routes = {
    # 'celery_tasks.bittrex_fetchOHLCV_task': {'queue': 'bittrex'},
    'celery_app.celery_tasks.bitfinex_fetch_ohlcvs_all_symbols': {'queue': 'bitfinex'}
}