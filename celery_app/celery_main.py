# This module contains the main Celery app

from celery import Celery


app = Celery('Celery Coin App')
app.config_from_object('celery_app.celery_config')
