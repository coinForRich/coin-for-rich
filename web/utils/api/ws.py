import asyncio
import redis
from threading import Thread
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session
from common.helpers.datetimehelpers import seconds
from common.config.constants import REDIS_DELIMITER
from fetchers.config.constants import WS_SERVE_REDIS_KEY
from web.config.constants import OHLCV_INTERVALS, WS_SERVE_EVENT_TYPES
import web.utils.api.rest as webapi_rest


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

class WSServerSender:
    '''
    Websocket sender for clients that subscribe to our web
    '''

    def __init__(
        self,
        ws_manager: WSServerConnectionManager,
        ws: WebSocket,
        redis_client: redis.Redis,
        db: Session
    ):
        self.loop_handler = AsyncLoopThread()
        self.loop_handler.start()
        self.ws_manager = ws_manager
        self.ws = ws
        self.redis_client = redis_client
        self.db = db
        self.serving_ids : List[str] = []
        
    async def _serve_ohlc(
            self, exchange: str, base_id: str, quote_id: str, interval: str
        ):
        '''
        Coroutine that serves OHLC from Redis hash or REST API
        Serves OHLC with timestamp in seconds
        Serves OHLC data every 1 second
        :params:
            
        '''

        if interval not in OHLCV_INTERVALS:
            await self.ws.send_json({
                'message': "interval must be in the determined list"
            })
        serving_id = f'ohlc_{exchange}_{base_id}_{quote_id}_{interval}'
        self.serving_ids.append(serving_id)
        while self.ws in self.ws_manager.active_connections and \
            serving_id in self.serving_ids:
            # If `interval` == "1m", use "fresh" data from Redis,
            #   otherwise use data from REST API
            # Sleep between messages according to `interval` as well
            if interval == "1m":
                ws_serve_redis_key = WS_SERVE_REDIS_KEY.format(
                    exchange = exchange,
                    delimiter = REDIS_DELIMITER,
                    base_id = base_id,
                    quote_id = quote_id
                )
                data = self.redis_client.hgetall(ws_serve_redis_key)
                if data:
                    try:
                        parsed_data = {
                            'time': seconds(int(data['time'])),
                            'open': float(data['open']),
                            'high': float(data['high']),
                            'low': float(data['low']),
                            'close': float(data['close'])
                        }
                        await self.ws.send_json(parsed_data)
                        print(f"Sending {parsed_data}")
                    except Exception as exc:
                        print(f"Serve OHLC: EXCEPTION: {exc}")    
                await asyncio.sleep(1)
            else:
                data = webapi_rest.get_ohlc(
                    db = self.db,
                    exchange = exchange,
                    base_id = base_id, 
                    quote_id = quote_id,
                    interval  = interval,
                    limit = 1
                )
                if data:
                    data = data[0]
                    data['open'] = float(data['open'])
                    data['high'] = float(data['high'])
                    data['low'] = float(data['low'])
                    data['close'] = float(data['close'])
                    try:
                        await self.ws.send_json(data)
                        print(f"Sending {data}")
                    except Exception as exc:
                        print(f"Serve OHLC: EXCEPTION: {exc}")
                        print(data)
                if interval == "5m":
                    await asyncio.sleep(5)
                elif interval == "15m":
                    await asyncio.sleep(15)
                elif interval == "30m":
                    await asyncio.sleep(30)
                elif interval == "1h":
                    await asyncio.sleep(60)
                elif interval == "6h":
                    await asyncio.sleep(360)
                elif interval == "12h":
                    await asyncio.sleep(720)
                elif interval == "1D":
                    await asyncio.sleep(1440)
                elif interval == "7D":
                    await asyncio.sleep(10080)

    def serve_ohlc(
            self, exchange: str, base_id: str, quote_id: str, interval: str
        ):
        '''
        Websocket API for serving OHLC
        '''

        asyncio.run_coroutine_threadsafe(
            self._serve_ohlc(
                exchange, base_id, quote_id, interval
            ),
            self.loop_handler.loop
        )
    
    async def _unserve_ohlc(
        self,
        exchange: str,
        base_id: str,
        quote_id: str,
        interval: str
    ):
        '''
        Stops serving OHLC for `exchange`, `base_id`, quote_id`, `interval`
        '''
        
        if interval not in OHLCV_INTERVALS:
            await self.ws.send_json({
                'message': "interval must be in the determined list"
            })
            return
        serving_id = f'ohlc_{exchange}_{base_id}_{quote_id}_{interval}'
        self.serving_ids.remove(serving_id)

    def unserve_ohlc(
        self,
        exchange: str,
        base_id: str,
        quote_id: str,
        interval: str
    ):
        '''
        Websocket API for stopping serving OHLC
        '''
        
        asyncio.run_coroutine_threadsafe(
            self._unserve_ohlc(
                exchange, base_id, quote_id, interval
            ),
            self.loop_handler.loop
        )