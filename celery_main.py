# This module contains the main Celery app

from celery import Celery


app = Celery('fetchers')
app.config_from_object('celery_config')
