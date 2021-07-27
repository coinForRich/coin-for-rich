# This module contains the main Celery app

from celery import Celery


# Celery main app
app = Celery('Celery Coin App')
app.config_from_object('celery_app.celery_config')

# Periodic OHLCV update
app.conf.beat_schedule = {
    'bitfinex_ohlcv_1min': {
        'task': "celery_app.celery_tasks.bitfinex_fetch_ohlcvs_mutual_basequote_1min",
        'schedule': 100.0
    },
    'binance_ohlcv_1min': {
        'task': "celery_app.celery_tasks.binance_fetch_ohlcvs_mutual_basequote_1min",
        'schedule': 60.0
    },
    'bittrex_ohlcv_1min': {
        'task': "celery_app.celery_tasks.bittrex_fetch_ohlcvs_mutual_basequote_1min",
        'schedule': 100.0
    }
}
