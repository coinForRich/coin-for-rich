# This module contains settings for Celery app

# Broker settings
broker_url = "amqp://coin_user:yummyicecream@localhost:5672/coin_fetcher"

# List of modules to import when the Celery worker starts
include = ['celery_tasks']

# Using the database to store task state
# TODO: choose result backend
result_backend = ""

# Routing
task_routes = {
    'celery_tasks.bittrex_fetchOHLCV_task': {'queue': 'bittrex'},
    'celery_tasks.bittrex_fetchOHLCV_OnDemand_task': {'queue': 'bittrex'},
    'celery_tasks.bitfinex_fetchOHLCV_task': {'queue': 'bitfinex'}
}   