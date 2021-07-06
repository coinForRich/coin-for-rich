import asyncio
import redis
from typing import Optional
from sqlalchemy.engine import base
from sqlalchemy.orm import Session
from fastapi import (
    FastAPI, Request, WebSocket, Depends, FastAPI, HTTPException
)
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.websockets import WebSocketDisconnect
from common.config.constants import (
    REDIS_HOST, REDIS_PASSWORD, DEFAULT_DATETIME_STR_QUERY
)
from common.helpers.datetimehelpers import str_to_datetime
from fetchers.config.constants import WS_SERVE_REDIS_KEY
from web import models
from web.helpers import dbhelpers
from web.database import SessionLocal, engine
from web.utils.websocketutils import WSConnectionManager, WSSender
from web.utils import webapiutils


models.Base.metadata.create_all(bind=engine)
app = FastAPI()
app.mount("/src", StaticFiles(directory="web/src"), name="src")
templates = Jinja2Templates(directory="web/templates")
ws_manager = WSConnectionManager()
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

@app.get("/testws", response_class=HTMLResponse)
async def testws(request: Request):
    return templates.TemplateResponse("testws.html", {"request": request})

@app.get("/ohlc/", name="read_ohlc")
async def read_ohlcv(
    exchange: str,
    base_id: str,
    quote_id: str,
    start: str,
    end: str,
    limit: Optional[int] = 500,
    db: Session = Depends(get_db)
    ):
    '''
    Reads OHLC from database, max 500 data points
    API endpoint for charting
    '''
    
    start_dt = str_to_datetime(start, DEFAULT_DATETIME_STR_QUERY)
    end_dt = str_to_datetime(end, DEFAULT_DATETIME_STR_QUERY)
    ohlcv = webapiutils.get_ohlc(
        db, exchange, base_id, quote_id, start_dt, end_dt, limit
    )
    if not ohlcv:
        raise HTTPException(status_code=404, detail="OHLCV not found")
    return ohlcv

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    ws_sender = WSSender()
    try:
        while True:
            input = await websocket.receive_text()
            if input:
                try:
                    exchange, symbol = input.split(";")
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
