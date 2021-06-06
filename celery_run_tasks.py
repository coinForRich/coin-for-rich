# This module runs Celery tasks


import datetime
from celery_tasks import *


if __name__ == "__main__":
    # bittrex_fetchOHLCV_OnDemand_task.delay(symbol="BTC-USD", start_date=datetime.datetime(2020, 3, 17), end_date=datetime.datetime(2021, 5, 19))
    # bitfinex_fetchOHLCV_OnDemand_task.delay(symbol="BTCUSD", start_date=datetime.datetime(2020, 5, 15), end_date=datetime.datetime(2021, 5, 19))
    get_and_fetch_all_task.delay()
