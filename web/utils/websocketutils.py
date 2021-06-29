import asyncio
from threading import Thread
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from common.helpers.datetimehelpers import seconds


class WSConnectionManager:
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

class AsyncLoopThread(Thread):
    '''
    Async loop thread for adding coroutines to
        a different OS thread
    '''

    def __init__(self):
        super().__init__(daemon=True)
        self.loop = asyncio.new_event_loop()

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
        return self.loop

class WSSender:
    '''
    Websocket sender for clients that subscribe to our web
    '''

    def __init__(self):
        self.loop_handler = AsyncLoopThread()
        self.loop_handler.start()

    async def _serve_ohlc_from_redis_hash(
            self, ws, ws_manager, redis_client, rkey
        ):
        '''
        Coroutine that serves OHLC from Redis hash
        :params:
            `ws`: fastAPI WebSocket obj
            `ws_manager`: WSConnectionManager obj
            `redis_client`: Redis client
            `rkey`: Redis key
        '''
        
        # TODO: make sure ws client is open
        while ws in ws_manager.active_connections:
            data = redis_client.hgetall(rkey)
            if data:
                try:
                    parsed_data = {
                        'time': seconds(int(data['time'])),
                        'open': float(data['open']),
                        'high': float(data['high']),
                        'low': float(data['low']),
                        'close': float(data['close'])
                    }
                    await ws.send_json(parsed_data)
                except Exception as exc:
                    print(exc)
            await asyncio.sleep(3)

    def serve_ohlc_from_redis_hash(
            self, ws, ws_manager, redis_client, rkey
        ):
        '''
        Serves OHLCV from Redis hash
        '''

        asyncio.run_coroutine_threadsafe(
            self._serve_ohlc_from_redis_hash(
                ws, ws_manager, redis_client, rkey),
            self.loop_handler.loop
        )