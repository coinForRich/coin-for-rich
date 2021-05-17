# This module runs Celery tasks


from celery_tasks import *


if __name__ == "__main__":
    bittrex_fetchOHLCV_task.delay()
    bitfinex_fetchOHLCV_task.delay()