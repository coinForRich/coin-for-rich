import json
from typing import Optional, Union
from datetime import datetime
from sqlalchemy.orm import Session
from common.helpers.datetimehelpers import datetime_to_seconds, milliseconds_to_datetime
from web import models
from web.config.constants import OHLCV_INTERVALS


def get_symbol_from_basequote(
        db: Session,
        exchange: str,
        base_id: str,
        quote_id: str
    ):
    '''
    Gets symbol from `base_id` and `quote_id`
    :params:

    '''

    result = db.query(models.SymbolExchange).filter(
        models.SymbolExchange.exchange == exchange,
        models.SymbolExchange.base_id == base_id,
        models.SymbolExchange.quote_id == quote_id
    ).one_or_none()
    symbol = result.symbol
    return symbol

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
        start: Union[datetime, int],
        end: Union[datetime, int],
        interval: str,
        limit: int = 500
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
        `start`: datetime obj or int - start time (must have same type as `end`)
        `end`: datetime obj or int - end time (must have same type as `start`)
        `interval`: str - time interval of candles
            enum: within the `OHLCV_INTERVALS` list
        `limit`: maximum number of data points to return
    '''

    # TODO: Also add input data type checking
    limit = min(limit, 500)
    if isinstance(start, int):
        start = milliseconds_to_datetime(start)
        end = milliseconds_to_datetime(end)
    if interval not in OHLCV_INTERVALS:
        raise ValueError("interval paramater must be in the defined list")
    elif interval == "1m":
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
