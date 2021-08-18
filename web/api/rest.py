import datetime
from typing import Optional, Union
from sqlalchemy import func, literal_column, literal
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql.functions import concat
from sqlalchemy.orm import Session, contains_alias
from common.helpers.datetimehelpers import (
    datetime_to_milliseconds,
    datetime_to_seconds,
    milliseconds_to_datetime
)
from web import models
from web.config.constants import OHLCV_INTERVALS


def get_symbol_from_ebq(
        db: Session,
        exchange: str,
        base_id: str,
        quote_id: str
    ):
    '''
    Gets symbol from `exchange`, `base_id` and `quote_id`
    :params:

    '''

    result = db.query(models.SymbolExchange).filter(
        models.SymbolExchange.exchange == exchange,
        models.SymbolExchange.base_id == base_id,
        models.SymbolExchange.quote_id == quote_id
    ).one_or_none()
    symbol = result.symbol
    return symbol

def parse_ohlcv(ohlcv: list, mls: bool) -> list:
    '''
    Parses OHLCV received from `get_ohlcv`
        for web chart view by converting timestamp
        into seconds
    
    :params:
        `ohlcv`: list - OHLCVs received
        `mls`: bool - whether to convert timestamps to milliseconds
            if true: convert to milliseconds
            if false: convert to seconds
    '''

    ret = []
    if ohlcv:
        ohlcv.sort(key = lambda x: x.time)
        ret = [
            {
                'time': int(datetime_to_milliseconds(o.time)) if mls \
                    else int(datetime_to_seconds(o.time)),
                'open': float(o.open),
                'high': float(o.high),
                'low': float(o.low),
                'close': float(o.close),
                'volume': float(o.volume)
            }
            for o in ohlcv
        ]
    return ret

def get_ohlcv(
        db: Session,
        exchange: str,
        base_id: str,
        quote_id: str,
        interval: str,
        start: Optional[Union[datetime.datetime, int]]=None,
        end: Optional[Union[datetime.datetime, int]]=None,
        limit: int=100,
        empty_ts: bool=False,
        results_mls: bool=True
    ) -> Union[list, None]:
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
        `empty_ts`: show timestamps with empty ohlcv values by
            filling them with the averages of the ohlcv values
            from resulted rows from database - this is for a correct chart
    
    If `interval` == `1m`: get directly from ohlcv table
    '''

    # TODO: Also add input data type checking
    limit = min(limit, 100)
    if end:
        end = milliseconds_to_datetime(end)
    else:
        end = datetime.datetime.now()
    if start:
        start = milliseconds_to_datetime(start)    
    
    # Return ohlcv from different tables based on interval
    if interval not in OHLCV_INTERVALS:
        return None
    elif interval == "1m":
        if not start:
            start = end - datetime.timedelta(minutes = limit)

        # Get max. latest XXXX rows from psql db
        fromdb = db.query(models.Ohlcv).filter(
            models.Ohlcv.exchange == exchange,
            models.Ohlcv.base_id == base_id,
            models.Ohlcv.quote_id == quote_id,
            models.Ohlcv.time >= start,
            models.Ohlcv.time <= end
        ) \
        .order_by(models.Ohlcv.time.desc()) \
        .limit(limit) \
        # .subquery()
        
        if empty_ts:
            # Convert fromdb
            fromdb = fromdb.subquery()

            # Dummy timestamps to fill empty timestamps from db
            dseries = db.query(
                func.generate_series(
                    func.min(fromdb.c.time),
                    func.max(fromdb.c.time),
                    func.cast(concat(1, ' MINUTE'), INTERVAL)
                ).label('time'),
                func.avg(fromdb.c.open).label('open'),
                func.avg(fromdb.c.high).label('high'),
                func.avg(fromdb.c.low).label('low'),
                func.avg(fromdb.c.close).label('close'),
                literal(0).label('volume')
            ).subquery()

            # Join the dummy timestamps with fromdb
            result = db.query(
                dseries.c.time,
                func.coalesce(fromdb.c.open, dseries.c.open).label('open'),
                func.coalesce(fromdb.c.high, dseries.c.high).label('high'),
                func.coalesce(fromdb.c.low, dseries.c.low).label('low'),
                func.coalesce(fromdb.c.close, dseries.c.close).label('close'),
                func.coalesce(fromdb.c.volume, dseries.c.volume).label('volume')
            ) \
            .outerjoin(fromdb, dseries.c.time == fromdb.c.time) \
            .order_by(dseries.c.time) \
            .all()
        else:
            result = fromdb.all()

    else:
        # Choose summary table according to `interval`
        # TODO: replace these `elifs` with lookup table
        if interval == "5m":
            if not start:
                start = end - datetime.timedelta(minutes = limit * 5)
            table = models.t_ohlcvs_summary_5min
            conc_tup = (5, ' MINUTES')
        elif interval == "15m":
            if not start:
                start = end - datetime.timedelta(minutes = limit * 15)
            table = models.t_ohlcvs_summary_15min
            conc_tup = (15, ' MINUTES')
        elif interval == "30m":
            if not start:
                start = end - datetime.timedelta(minutes = limit * 30)
            table = models.t_ohlcvs_summary_30min
            conc_tup = (30, ' MINUTES')
        elif interval == "1h":
            if not start:
                start = end - datetime.timedelta(hours = limit * 1)
            table = models.t_ohlcvs_summary_1hour
            conc_tup = (1, ' HOUR')
        elif interval == "6h":
            if not start:
                start = end - datetime.timedelta(hours = limit * 6)
            table = models.t_ohlcvs_summary_6hour
            conc_tup = (6, ' HOURS')
        elif interval == "12h":
            if not start:
                start = end - datetime.timedelta(hours = limit * 12)
            table = models.t_ohlcvs_summary_12hour
            conc_tup = (12, ' HOURS')
        elif interval == "1D":
            if not start:
                start = end - datetime.timedelta(days = limit * 1)
            table = models.t_ohlcvs_summary_daily
            conc_tup = (1, ' DAY')
        elif interval == "7D":
            if not start:
                start = end - datetime.timedelta(days = limit * 7)
            table = models.t_ohlcvs_summary_7day
            conc_tup = (7, ' DAYS')

        fromdb = db.query(
            table.c.bucket.label('time'),
            table.c.open.label('open'),
            table.c.high.label('high'),
            table.c.low.label('low'),
            table.c.close.label('close'),
            table.c.volume.label('volume')
        ) \
        .filter(
            table.c.exchange == exchange,
            table.c.base_id == base_id,
            table.c.quote_id == quote_id,
            table.c.bucket >= start,
            table.c.bucket <= end
        ) \
        .order_by(table.c.bucket.desc()) \
        .limit(limit) \
        # .subquery()

        if empty_ts:
            # Convert fromdb
            fromdb = fromdb.subquery()

            # Dummy timestamps to fill empty timestamps from db
            dseries = db.query(
                func.generate_series(
                    func.min(fromdb.c.time),
                    func.max(fromdb.c.time),
                    func.cast(concat(conc_tup[0], conc_tup[1]), INTERVAL)
                ).label('time'),
                func.avg(fromdb.c.open).label('open'),
                func.avg(fromdb.c.high).label('high'),
                func.avg(fromdb.c.low).label('low'),
                func.avg(fromdb.c.close).label('close'),
                literal(0).label('volume')
            ).subquery()

            # Join the dummy timestamps with fromdb
            result = db.query(
                dseries.c.time,
                func.coalesce(fromdb.c.open, dseries.c.open).label('open'),
                func.coalesce(fromdb.c.high, dseries.c.high).label('high'),
                func.coalesce(fromdb.c.low, dseries.c.low).label('low'),
                func.coalesce(fromdb.c.close, dseries.c.close).label('close'),
                func.coalesce(fromdb.c.volume, dseries.c.volume).label('volume')
            ) \
            .outerjoin(fromdb, dseries.c.time == fromdb.c.time) \
            .order_by(dseries.c.time) \
            .all()
        else:
            result = fromdb.all()
        
    return parse_ohlcv(result, results_mls)
