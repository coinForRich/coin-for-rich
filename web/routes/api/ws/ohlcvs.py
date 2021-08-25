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
from web.routes.api.ws.utils.connections import WSServerConnectionManager


class WSServerSender:
    '''
    Websocket sender for clients that view chart
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
        self.serving_ids: List[str] = []
        
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
        # TODO: This serving_id is not unique among different users/clients
        serving_id = f'ohlc_{exchange}_{base_id}_{quote_id}_{interval}'
        self.serving_ids.append(serving_id)
        while self.ws in self.ws_manager.active_connections and \
            serving_id in self.serving_ids:
            # If `interval` == "1m", use "fresh" data from Redis,
            #   otherwise use data from REST API
            # Sleep between messages according to `interval` as well
            # TODO: add heartbeat
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
                data = await ohlcvs.read_ohlcvs(
                    exchange = exchange,
                    base_id = base_id, 
                    quote_id = quote_id,
                    interval = interval,
                    limit = 1,
                    empty_ts = True,
                    results_mls = False,
                    db = self.db
                )
                if data:
                    data = data[0]
                    # data['open'] = float(data['open'])
                    # data['high'] = float(data['high'])
                    # data['low'] = float(data['low'])
                    # data['close'] = float(data['close'])
                    try:
                        await self.ws.send_json(data)
                        print(f"Sending {data}")
                    except Exception as exc:
                        print(f"Serve OHLC: EXCEPTION: {exc}")
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