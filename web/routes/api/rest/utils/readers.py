# Get functions from database, independent of backend path operations

import datetime
from typing import List, Optional, Union
from sqlalchemy import func, literal, column
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql.functions import concat
from common.helpers.datetimehelpers import milliseconds_to_datetime
from web import models
from web.config.constants import OHLCV_INTERVALS
from web.routes.api.rest.utils import caching, parsers


def read_test(db: Session) -> List[models.TestTable]:
    '''
    Reads a row from `test` table
    '''
    
    return db.query(models.TestTable) \
        .order_by(models.TestTable.id).limit(1).all()

def read_symbol_exchange(db: Session) -> List[models.SymbolExchange]:
    '''
    Reads all rows from `symbol_exchange` database table
    '''

    return db.query(models.SymbolExchange) \
        .order_by(models.SymbolExchange.exchange.asc()).all()

def read_top500dr(db: Session) -> list:
    '''
    Reads all rows from `top_500_daily_return` database table
    '''

    return db.query(models.top_500_daily_return) \
        .order_by(models.top_500_daily_return.c.ranking).all()

def read_ohlcvs(
        db: Session,
        exchange: str,
        base_id: str,
        quote_id: str,
        interval: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: Optional[int] = 500,
        empty_ts: Optional[bool] = False,
        results_mls: Optional[bool] = True,
    ) -> list:
    '''
    Reads rows from `ohlcvs` database table,
        based on `exchange`, `base_id`, `quote_id`
        and timestamps between `start` and `end` (inclusive)
    
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
        `empty_ts`: show timestamps with empty ohlcv values by filling them with the averages of the ohlcv values from resulted rows from database - this is for a more "correct" chart;
            If this parameter is `true`, results are sorted in ascending order, so that the real-time chart engine runs without errors
        `results_mls`: bool if result timestamps are in milliseconds;
            If true, result timestamps are in millieconds;
            If false, result timestamps are in seconds
    
    If `interval` == `1m`: get directly from ohlcv table
    '''

    limit = min(limit, 500)
    if end:
        end = milliseconds_to_datetime(end).replace(second=0, microsecond=0)
    else:
        end = (
            datetime.datetime.now() - datetime.timedelta(minutes = 1)
        ).replace(second=0, microsecond=0)
    if start:
        start = milliseconds_to_datetime(start).replace(second=0, microsecond=0)

    # Return ohlcv from different tables based on interval
    ohlcvs = []
    if interval not in OHLCV_INTERVALS:
        return ohlcvs
    # Get max. latest XXXX rows from psql db
    elif interval == "1m":
        if start:
            fromdb = db.query(models.Ohlcv) \
                .options(caching.FromCache("default")) \
                .filter(
                    models.Ohlcv.exchange == exchange,
                    models.Ohlcv.base_id == base_id,
                    models.Ohlcv.quote_id == quote_id,
                    models.Ohlcv.time >= start,
                    models.Ohlcv.time <= end
                ) \
                .order_by(models.Ohlcv.time.desc()) \
                .limit(limit) \
                # .subquery()
        else:
            fromdb = db.query(models.Ohlcv) \
                .options(caching.FromCache("default")) \
                .filter(
                    models.Ohlcv.exchange == exchange,
                    models.Ohlcv.base_id == base_id,
                    models.Ohlcv.quote_id == quote_id,
                    models.Ohlcv.time <= end
                ) \
                .order_by(models.Ohlcv.time.desc()) \
                .limit(limit) \
                # .subquery()
        
        if empty_ts:
            # Convert fromdb
            fromdb = fromdb.subquery()

            # Dummy timestamps to fill empty timestamps from db
            ts = func.generate_series(
                    func.min(fromdb.c.time),
                    end,
                    func.cast(concat(1, ' MINUTE'), INTERVAL)
                ).label('time')
            
            dseries = db.query(
                ts,
                func.avg(fromdb.c.open).label('open'),
                func.avg(fromdb.c.high).label('high'),
                func.avg(fromdb.c.low).label('low'),
                func.avg(fromdb.c.close).label('close'),
                literal(0).label('volume')) \
                .order_by(ts.desc()) \
                .limit(limit) \
                .subquery()

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
            .order_by(dseries.c.time.asc()) \
            .limit(limit) \
            .all()
        else:
            result = fromdb.all()

    else:
        # Choose summary table according to `interval`
        if interval == "5m":
            # if not start:
            #     start = end - datetime.timedelta(minutes = limit * 5)
            table = models.t_ohlcvs_summary_5min
            conc_tup = (5, ' MINUTES')
        elif interval == "15m":
            # if not start:
            #     start = end - datetime.timedelta(minutes = limit * 15)
            table = models.t_ohlcvs_summary_15min
            conc_tup = (15, ' MINUTES')
        elif interval == "30m":
            # if not start:
            #     start = end - datetime.timedelta(minutes = limit * 30)
            table = models.t_ohlcvs_summary_30min
            conc_tup = (30, ' MINUTES')
        elif interval == "1h":
            # if not start:
            #     start = end - datetime.timedelta(hours = limit * 1)
            table = models.t_ohlcvs_summary_1hour
            conc_tup = (1, ' HOUR')
        elif interval == "6h":
            # if not start:
            #     start = end - datetime.timedelta(hours = limit * 6)
            table = models.t_ohlcvs_summary_6hour
            conc_tup = (6, ' HOURS')
        elif interval == "12h":
            # if not start:
            #     start = end - datetime.timedelta(hours = limit * 12)
            table = models.t_ohlcvs_summary_12hour
            conc_tup = (12, ' HOURS')
        elif interval == "1D":
            # if not start:
            #     start = end - datetime.timedelta(days = limit * 1)
            table = models.t_ohlcvs_summary_daily
            conc_tup = (1, ' DAY')
        elif interval == "7D":
            # if not start:
            #     start = end - datetime.timedelta(days = limit * 7)
            table = models.t_ohlcvs_summary_7day
            conc_tup = (7, ' DAYS')

        if start:
            fromdb = db.query(
                table.c.bucket.label('time'),
                table.c.open.label('open'),
                table.c.high.label('high'),
                table.c.low.label('low'),
                table.c.close.label('close'),
                table.c.volume.label('volume')) \
                .options(caching.FromCache("default")) \
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
        else:
            fromdb = db.query(
                table.c.bucket.label('time'),
                table.c.open.label('open'),
                table.c.high.label('high'),
                table.c.low.label('low'),
                table.c.close.label('close'),
                table.c.volume.label('volume')) \
                .options(caching.FromCache("default")) \
                .filter(
                    table.c.exchange == exchange,
                    table.c.base_id == base_id,
                    table.c.quote_id == quote_id,
                    table.c.bucket <= end
                ) \
                .order_by(table.c.bucket.desc()) \
                .limit(limit) \
                # .subquery()

        if empty_ts:
            # Convert fromdb
            fromdb = fromdb.subquery()

            # Dummy timestamps to fill empty timestamps from db
            ts = func.generate_series(
                    func.min(fromdb.c.time),
                    end,
                    func.cast(concat(conc_tup[0], conc_tup[1]), INTERVAL)
                ).label('time')

            dseries = db.query(
                ts,
                func.avg(fromdb.c.open).label('open'),
                func.avg(fromdb.c.high).label('high'),
                func.avg(fromdb.c.low).label('low'),
                func.avg(fromdb.c.close).label('close'),
                literal(0).label('volume')) \
                .order_by(ts.desc()) \
                .limit(limit) \
                .subquery()

            # Join the dummy timestamps with fromdb
            result = db.query(
                dseries.c.time,
                func.coalesce(fromdb.c.open, dseries.c.open).label('open'),
                func.coalesce(fromdb.c.high, dseries.c.high).label('high'),
                func.coalesce(fromdb.c.low, dseries.c.low).label('low'),
                func.coalesce(fromdb.c.close, dseries.c.close).label('close'),
                func.coalesce(fromdb.c.volume, dseries.c.volume).label('volume')) \
                .outerjoin(fromdb, dseries.c.time == fromdb.c.time) \
                .order_by(dseries.c.time.asc()) \
                .limit(limit) \
                .all()
        else:
            result = fromdb.all()
    
    # Parse ohlcvs
    ohlcvs = parsers.parse_ohlcv(result, results_mls)
    return ohlcvs
