# This module contains Celery tasks


from celery_main import app
from fetchers import bitfinex_fetchOHLCV
from fetchers import bittrex_fetchOHLCV


@app.task
def bittrex_fetchOHLCV_task():
    bittrex_fetchOHLCV.run()

@app.task
def bitfinex_fetchOHLCV_task():
    bitfinex_fetchOHLCV.run()