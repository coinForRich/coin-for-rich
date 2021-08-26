# Backend WS API endpoint for OHLCV

from redis import Redis
from typing import Optional
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, WebSocket
from fastapi.exceptions import HTTPException
from starlette.websockets import WebSocketDisconnect
from web.config.constants import WS_SEND_EVENT_TYPES
from web.routes.api.deps import get_db, get_redis
from web.routes.api.rest.utils import readers
from web.routes.api.ws.utils.senders import WSSender
from web.routes.api.ws.utils.connections import WSConnectionManager


router = APIRouter()
ws_manager = WSConnectionManager()

@router.websocket("/ohlcvs")
async def ws_ohlcvs(
        websocket: WebSocket,
        redis_client: Redis = Depends(get_redis),
        db: Session = Depends(get_db)
    ):

    await ws_manager.connect(websocket)
    ws_sender = WSSender(ws_manager, websocket, redis_client, db)
    try:
        while True:
            input = await websocket.receive_json()
            if input:
                try:
                    event_type = input['event_type']
                    data_type = input['data_type']
                    exchange = input['exchange']
                    base_id = input['base_id']
                    quote_id = input['quote_id']
                    interval = input['interval']
                    if event_type not in WS_SEND_EVENT_TYPES:
                        await websocket.send_json({
                            'detail': "event_type must be subscribe or unsubscribe"
                        })
                    elif event_type == "subscribe":
                        mls = input['mls'] # only subsribe requires mls
                        if data_type == "ohlcv":
                            print(f"subscribing to {interval}")
                            ws_sender.send_ohlcv(
                                exchange, base_id, quote_id, interval, mls)
                    elif event_type == "unsubscribe":
                        if data_type == "ohlcv":
                            ws_sender.stopsend_ohlcv(
                                exchange, base_id, quote_id, interval)
                            await websocket.send_json({
                                'detail': \
                                    f"successfully unsubscribed from {exchange}_{base_id}_{quote_id}_{interval}"})
                except Exception as exc:
                    print(f'Websocket OHLCVS: EXCEPTION: {exc}')
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
