import asyncio
import time
import redis
from os import truncate
from typing import Optional, Union
from sqlalchemy.engine import base
from sqlalchemy.orm import Session
from fastapi import (
    FastAPI, Request, WebSocket, Depends, FastAPI, HTTPException
)
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.sql.operators import endswith_op
from starlette.websockets import WebSocketDisconnect
from common.config.constants import (
    REDIS_DELIMITER, REDIS_HOST, REDIS_PASSWORD, DEFAULT_DATETIME_STR_QUERY
)
from common.helpers.datetimehelpers import str_to_datetime, str_to_milliseconds
from fetchers.config.constants import WS_SERVE_REDIS_KEY
from web import models
from web.helpers import dbhelpers
from web.database import SessionLocal, engine
from web.api.ws import WSServerConnectionManager, WSServerSender
import web.api.rest as webapi_rest
from web.config.constants import WS_SERVE_EVENT_TYPES


models.Base.metadata.create_all(bind=engine)
app = FastAPI()
app.mount("/src", StaticFiles(directory="web/src"), name="src")
templates = Jinja2Templates(directory="web/templates")
ws_manager = WSServerConnectionManager()
redis_client = redis.Redis(
    host=REDIS_HOST,
    username="default",
    password=REDIS_PASSWORD,
    decode_responses=True)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/testws", response_class=HTMLResponse)
async def testws(
        request: Request,
        exchange: str,
        base_id: str,
        quote_id: str
    ):
    return templates.TemplateResponse(
        "viewsymbol.html", {
            "request": request,
            "exchange": exchange,
            "base_id": base_id,
            "quote_id": quote_id
        }
    )

@app.get("/ohlcv/", name="read_ohlcv")
async def read_ohlcv(
        exchange: str,
        base_id: str,
        quote_id: str,
        interval: str,
        start: Optional[Union[str, int]] = None,
        end: Optional[Union[str, int]] = None,
        mls: Optional[bool] = True,
        limit: Optional[int] = 100,
        empty_ts: Optional[bool] = False,
        results_mls: Optional[bool] = True,
        db: Session = Depends(get_db)
    ):
    '''
    API to read OHLCV from database, max XXXX data points
    
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
        
        `mls`: bool if query is using milliseconds
            if true, both `start` and `end` must be int

        `results_mls`: bool if result timestamps are in milliseconds
            if true, result timestamps are in millieconds
            if false, result timestamps are in seconds

    example of output:
        ```
        [{'time': 1629296700000, 'open': 2619.4, 'high': 2619.4, 'low': 2619.4, 'close': 2619.4, 'volume': 0.02230204}, ...]
        ```
    '''
    
    if mls:
        start = int(start) if start else start
        end = int(end) if end else end
    else:
        start = str_to_datetime(start, DEFAULT_DATETIME_STR_QUERY) if start else start
        end = str_to_datetime(end, DEFAULT_DATETIME_STR_QUERY) if end else end
    ohlcv = webapi_rest.get_ohlcv(
        db, exchange, base_id, quote_id,
        interval, start, end, limit, empty_ts, results_mls
    )
    if not ohlcv:
        raise HTTPException(status_code=404, detail="OHLCV not found")
    return ohlcv

@app.websocket("/candles")
async def websocket_endpoint(
        websocket: WebSocket,
        db: Session = Depends(get_db)
    ):
    await ws_manager.connect(websocket)
    ws_sender = WSServerSender(ws_manager, websocket, redis_client, db)
    try:
        while True:
            input = await websocket.receive_json()
            if input:
                print(input)
                try:
                    event_type = input['event_type']
                    data_type = input['data_type']
                    exchange = input['exchange']
                    base_id = input['base_id']
                    quote_id = input['quote_id']
                    interval = input['interval']
                    if event_type not in WS_SERVE_EVENT_TYPES:
                        await websocket.send_json({
                            'message': "event_type must be subscribe or unsubscribe"
                        })
                    elif event_type == "subscribe":
                        if data_type == "ohlc":
                            ws_sender.serve_ohlc(
                                exchange, base_id, quote_id, interval
                            )
                    elif event_type == "unsubscribe":
                        if data_type == "ohlc":
                            ws_sender.unserve_ohlc(
                                exchange, base_id, quote_id, interval
                            )
                            await websocket.send_json({
                            'message': \
                                f"unsubscribed successfully from {exchange}_{base_id}_{quote_id}_{interval}"
                        })
                except Exception as exc:
                    print(f'EXCEPTION: {exc}')
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
