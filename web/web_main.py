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
    REDIS_HOST, REDIS_PASSWORD, DEFAULT_DATETIME_STR_QUERY
)
from common.helpers.datetimehelpers import str_to_datetime, str_to_milliseconds
from fetchers.config.constants import WS_SERVE_REDIS_KEY
from web import models
from web.helpers import dbhelpers
from web.database import SessionLocal, engine
from web.utils.api.ws import WSServerConnectionManager, WSServerSender
import web.utils.api.rest as webapi_rest


models.Base.metadata.create_all(bind=engine)
app = FastAPI()
app.mount("/src", StaticFiles(directory="web/src"), name="src")
templates = Jinja2Templates(directory="web/templates")
ws_manager = WSServerConnectionManager()
redis_client = redis.Redis(
    host=REDIS_HOST,
    username="default",
    password=REDIS_PASSWORD,
    decode_responses=True
)

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

@app.get("/testws/", response_class=HTMLResponse)
async def testws(
    request: Request,
    exchange: str,
    base_id: str,
    quote_id: str
    ):
    return templates.TemplateResponse(
        "testws.html", {
            "request": request,
            "exchange": exchange,
            "base_id": base_id,
            "quote_id": quote_id
        }
    )

@app.get("/ohlc/", name="read_ohlc")
async def read_ohlcv(
    exchange: str,
    base_id: str,
    quote_id: str,
    start: Union[str, int],
    end: Union[str, int],
    interval: str,
    mls: Optional[bool] = True,
    limit: Optional[int] = 500,
    db: Session = Depends(get_db)
    ):
    '''
    Reads OHLC from database, max 500 data points
    API endpoint for charting
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
        
        `mls`: bool if query is using milliseconds or not
            if `mls`, `start` and `end` must be int
    '''

    starts = time.time()
    if mls:
        start = int(start)
        end = int(end)
    else:
        start = str_to_datetime(start, DEFAULT_DATETIME_STR_QUERY)
        end = str_to_datetime(end, DEFAULT_DATETIME_STR_QUERY)
    
    ohlcv = webapi_rest.get_ohlc(
        db, exchange, base_id, quote_id, start, end, interval, limit
    )
    
    if not ohlcv:
        raise HTTPException(status_code=404, detail="OHLCV not found")
        # return None
    ends = time.time()
    print(f"Read ohlcv endpoint elapsed: {ends - starts} seconds")
    return ohlcv

@app.websocket("/candles")
async def websocket_endpoint(
    websocket: WebSocket,
    db: Session = Depends(get_db)
    ):
    await ws_manager.connect(websocket)
    ws_sender = WSServerSender()
    try:
        while True:
            input = await websocket.receive_json()
            if input:
                print(input)
                try:
                    data_type = input['data_type']
                    exchange = input['exchange']
                    base_id = input['base_id']
                    quote_id = input['quote_id']
                    symbol = webapi_rest.get_symbol_from_basequote(
                        db, exchange, base_id, quote_id
                    )
                    if data_type == "ohlc":
                        ws_sender.serve_ohlc_from_redis_hash(
                            websocket,
                            ws_manager,
                            redis_client,
                            WS_SERVE_REDIS_KEY.format(
                                exchange = exchange,
                                symbol = symbol
                            )
                        )
                except Exception as exc:
                    print(f'EXCEPTION: {exc}')
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
# aaa