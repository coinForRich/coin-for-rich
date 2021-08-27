# Backend REST API endpoint for OHLCV

from typing import Optional
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from web.routes.api.deps import get_db
from web.routes.api.rest.utils import readers


router = APIRouter()

@router.get("/ohlcvs", name="get_ohlcvs")
async def get_ohlcvs(
        exchange: str,
        base_id: str,
        quote_id: str,
        interval: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: Optional[int] = 500,
        empty_ts: Optional[bool] = False,
        results_mls: Optional[bool] = True,
        db: Session = Depends(get_db)
    ) -> list:
    '''
    API to read OHLCV from database, max 500 data points
    
    Can be used for charting
    
    :params:
        `interval`: str - time interval of candles
            enum:
            
                - `1m` for 1 minute
                - `5m` for 5 minutes
                - `15m` for 15 minutes
                - `30m` for 30 minutes
                - `1h` for 1 hour
                - `3h` for 3 hours
                - `6h` for 6 hours
                - `12h` for 12 hours
                - `1D` for 1 day
                - `7D` for 7 days
                - `14D` for 14 days
                - `1M` for 1 month 

        `results_mls`: bool if result timestamps are in milliseconds
            if true, result timestamps are in millieconds
            if false, result timestamps are in seconds

    example of output:
        ```
        [{'time': 1629296700000, 'open': 2619.4, 'high': 2619.4, 'low': 2619.4, 'close': 2619.4, 'volume': 0.02230204}, ...]
        ```
    '''

    return readers.read_ohlcvs(
        db, exchange, base_id, quote_id,
        interval, start, end, limit, empty_ts, results_mls
    )