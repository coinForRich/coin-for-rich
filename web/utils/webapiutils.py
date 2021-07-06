import json
from datetime import datetime
from os import times
from sqlalchemy.orm import Session
from web import models
from common.helpers.datetimehelpers import datetime_to_seconds


def parse_ohlc(ohlcv: list):
    '''
    Parses OHLC received from `get_ohlcv`
        for web chart view by converting timestamp
        into seconds
    :params:
        `ohlcv`: list - OHLCVs received
    '''
    if ohlcv:
        return [
            {
                'time': int(datetime_to_seconds(o.time)),
                'open': o.open,
                'high': o.high,
                'low': o.low,
                'close': o.close
            }
            for o in ohlcv
        ]
    return None

def get_ohlc(
        db: Session,
        exchange: str,
        base_id: str,
        quote_id: str,
        start: datetime,
        end: datetime,
        limit=500
    ):
    '''
    Gets OHLCV from psql based on exchange, base_id, quote_id
        and timestamp between start, end (inclusive)
    Limits to a maximum of 500 data points
    :params:
        `db`: sqlalchemy Session obj
        `exchange`: str - exchange name
        `base_id`: str - base id
        `quote_id`: str - quote id
        `start`: datetime obj - start time
        `end`: datetime obj - end time
        `limit`: maximum number of data points to return
    '''

    limit = min(limit, 500)
    results = db.query(models.Ohlcv).filter(
        models.Ohlcv.exchange == exchange,
        models.Ohlcv.base_id == base_id,
        models.Ohlcv.quote_id == quote_id,
        models.Ohlcv.time >= start,
        models.Ohlcv.time <= end
    )\
    .order_by(models.Ohlcv.time.asc())\
    .limit(limit).all()
    return parse_ohlc(results)
