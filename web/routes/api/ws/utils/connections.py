import asyncio
import redis
from typing import List
from sqlalchemy.orm import Session
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from common.helpers.datetimehelpers import seconds
from common.config.constants import REDIS_DELIMITER
from common.utils.asyncioutils import AsyncLoopThread
from fetchers.config.constants import WS_SERVE_REDIS_KEY
from web.config.constants import OHLCV_INTERVALS, WS_SERVE_EVENT_TYPES
from web.routes.api.rest import ohlcvs


class WSServerConnectionManager:
    '''
    Websocket connection manager
    '''

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    # async def send_personal_message(self, message: str, websocket: WebSocket):
    #     await websocket.send_text(message)

    # async def broadcast(self, message: str):
    #     for connection in self.active_connections:
    #         await connection.send_text(message)
