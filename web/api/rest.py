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

def parse_ohlc(ohlcv: list, summary: bool):
    '''
    Parses OHLC received from `get_ohlcv`
        for web chart view by converting timestamp
        into seconds
    :params:
        `ohlcv`: list - OHLCVs received
        `summary`: bool - whether it's summary/aggregated data or direct data
    '''
    if ohlcv:
        if not summary:
            ohlcv.sort(key = lambda x: x.time)
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
        else:
            ohlcv.sort(key = lambda x: x.bucket)
            return [
                {
                    'time': int(datetime_to_seconds(o.bucket)),
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
        interval: str,
        start: Optional[Union[datetime, int]] = None,
        end: Optional[Union[datetime, int]] = None,
        limit: int = 500
    ):
    '''
    Gets OHLCV from psql based on exchange, base_id, quote_id
        and timestamp between start, end (inclusive)
    
    Limits to a maximum of 500 data points
    
    Outputs rows with timestamp by ascending order
    
    Outputs rows with timestamp in seconds
    
    :params:
        `db`: sqlalchemy Session obj
        `exchange`: str - exchange name
        `base_id`: str - base id
        `quote_id`: str - quote id
        `interval`: str - time interval of candles
            enum: within the `OHLCV_INTERVALS` list
        `start`: datetime obj or int - start time (must have same type as `end`)
        `end`: datetime obj or int - end time (must have same type as `start`)
        `limit`: maximum number of data points to return
    If `interval` == `1m`: get directly from ohlcv table
    '''

    # TODO: Also add input data type checking
    limit = min(limit, 500)
    if isinstance(start, int):
        start = milliseconds_to_datetime(start)
    if isinstance(end, int):
        end = milliseconds_to_datetime(end)
    
    # Return ohlc from different sources based on interval
    if interval not in OHLCV_INTERVALS:
        raise ValueError("interval paramater must be in the defined list")
    elif interval == "1m":
        # Get max. latest 500 rows
        if start is None and end is None:
            results = db.query(models.Ohlcv).filter(
                models.Ohlcv.exchange == exchange,
                models.Ohlcv.base_id == base_id,
                models.Ohlcv.quote_id == quote_id
            ) \
                .order_by(models.Ohlcv.time.desc()) \
                .limit(limit).all()
        # Get max. latest 500 rows with time <= `end`
        elif start is None:
            results = db.query(models.Ohlcv).filter(
                models.Ohlcv.exchange == exchange,
                models.Ohlcv.base_id == base_id,
                models.Ohlcv.quote_id == quote_id,
                models.Ohlcv.time <= end
            ) \
                .order_by(models.Ohlcv.time.desc()) \
                .limit(limit).all()
        # Get max. oldest 500 rows with time >= `start`
        elif end is None:
            results = db.query(models.Ohlcv).filter(
                models.Ohlcv.exchange == exchange,
                models.Ohlcv.base_id == base_id,
                models.Ohlcv.quote_id == quote_id,
                models.Ohlcv.time >= start
            ) \
                .order_by(models.Ohlcv.time.asc()) \
                .limit(limit).all()
        # Get max. latest 500 rows with time between `start` and `end`
        else:
            results = db.query(models.Ohlcv).filter(
                models.Ohlcv.exchange == exchange,
                models.Ohlcv.base_id == base_id,
                models.Ohlcv.quote_id == quote_id,
                models.Ohlcv.time >= start,
                models.Ohlcv.time <= end
        ) \
                .order_by(models.Ohlcv.time.desc()) \
                .limit(limit).all()
        return parse_ohlc(results, False)
    else:
        # Choose summary table according to `interval`
        # TODO: replace these `elifs` with lookup table
        if interval == "5m":
            table = models.t_ohlcvs_summary_5min
        elif interval == "15m":
            table = models.t_ohlcvs_summary_15min
        elif interval == "30m":
            table = models.t_ohlcvs_summary_30min
        elif interval == "1h":
            table = models.t_ohlcvs_summary_1hour
        elif interval == "6h":
            table = models.t_ohlcvs_summary_6hour
        elif interval == "12h":
            table = models.t_ohlcvs_summary_12hour
        elif interval == "1D":
            table = models.t_ohlcvs_summary_daily
        elif interval == "7D":
            table = models.t_ohlcvs_summary_7day

        if start is None and end is None:
            results = db.query(table).filter(
                table.c.exchange == exchange,
                table.c.base_id == base_id,
                table.c.quote_id == quote_id
            ) \
            .order_by(table.c.bucket.desc()) \
            .limit(limit).all()
        elif start is None:
            results = db.query(table).filter(
                table.c.exchange == exchange,
                table.c.base_id == base_id,
                table.c.quote_id == quote_id,
                table.c.bucket <= end
            ) \
            .order_by(table.c.bucket.desc()) \
            .limit(limit).all()
        elif end is None:
            results = db.query(table).filter(
                table.c.exchange == exchange,
                table.c.base_id == base_id,
                table.c.quote_id == quote_id,
                table.c.bucket >= start
            ) \
            .order_by(table.c.bucket.asc()) \
            .limit(limit).all()
        else:
            results = db.query(table).filter(
                table.c.exchange == exchange,
                table.c.base_id == base_id,
                table.c.quote_id == quote_id,
                table.c.bucket >= start,
                table.c.bucket <= end
            ) \
            .order_by(table.c.bucket.desc()) \
            .limit(limit).all()
        return parse_ohlc(results, True)