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

@router.websocket("/test")
async def ws_test(websocket: WebSocket):
    '''
    API used for testing WS API
    '''

    await ws_manager.connect(websocket)
    await websocket.send_json({"detail": "Hello WebSocket"})
    await websocket.close()
