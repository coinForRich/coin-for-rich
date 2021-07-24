# This module contains the main Celery app

import datetime
from celery import Celery
from common.config.constants import DEFAULT_DATETIME_STR_QUERY
from common.helpers.datetimehelpers import datetime_to_str


app = Celery('Celery Coin App')
app.config_from_object('celery_app.celery_config')

# Periodic OHLCV update, every 2 minutes
app.conf.beat_schedule = {
    'bitfinex_ohlcv_1min': {
        'task': "celery_app.celery_tasks.bitfinex_fetch_ohlcvs_mutual_basequote_1min",
        'schedule': 70.0
    }
}
