# Backend WS API senders utils

import asyncio
import logging
from redis import Redis
from typing import List
from sqlalchemy.orm import Session
from fastapi import WebSocket
from common.config.constants import REDIS_DELIMITER
from common.utils.asyncioutils import AsyncLoopThread
from fetchers.helpers.ws import make_send_redis_key
from web.config.constants import OHLCV_INTERVALS
from web.routes.api.rest.utils.readers import read_ohlcvs
from web.routes.api.ws.utils.parsers import parse_ohlcv
from web.routes.api.ws.utils.connections import WSConnectionManager


class WSSender:
    '''
    Websocket sender for clients that view chart
    '''

    def __init__(
            self,
            ws_manager: WSConnectionManager,
            ws: WebSocket,
            redis_client: Redis,
            db: Session
        ):
        self.loop_handler = AsyncLoopThread()
        self.loop_handler.start()
        self.ws_manager = ws_manager
        self.ws = ws
        self.redis_client = redis_client
        self.db = db
        self.serving_ids: List[str] = []
        
    async def _send_ohlcv(
            self,
            exchange: str,
            base_id: str,
            quote_id: str,
            interval: str,
            mls: bool = True
        ) -> None:
        '''
        Private coroutine that sends OHLCV from Redis hash or REST API
        
        Sends OHLCV data every XXXX seconds
        
        :params:
            `mls`: boolean
                if True: sends timestamps in milliseconds
                if False: sends timestamps in seconds
            
        '''

        if interval not in OHLCV_INTERVALS:
            await self.ws.send_json({
                'message': "interval must be in the determined list"
            })
        
        # TODO: This serving_id is not unique among different users/clients
        serving_id = f'ohlcv_{exchange}_{base_id}_{quote_id}_{interval}'
        self.serving_ids.append(serving_id)
        
        while self.ws in self.ws_manager.active_connections and \
            serving_id in self.serving_ids:
            # If `interval` == "1m", use "fresh" data from Redis,
            #   otherwise use data from REST API
            # Sleep between messages according to `interval` as well
            # TODO: add heartbeat
            if interval == "1m":
                ws_send_redis_key = make_send_redis_key(
                    exchange, base_id, quote_id, REDIS_DELIMITER
                )
                data = self.redis_client.hgetall(ws_send_redis_key)
                if data:
                    try:
                        data = parse_ohlcv(data, mls)
                        await self.ws.send_json(data)
                    except Exception as exc:
                        logging.warning(f"Send OHLCV: EXCEPTION: {exc}")    
                await asyncio.sleep(1)
            else:
                data = read_ohlcvs(
                    db = self.db,
                    exchange = exchange,
                    base_id = base_id, 
                    quote_id = quote_id,
                    interval = interval,
                    limit = 1,
                    empty_ts = False,
                    results_mls = True,
                )

                if data:
                    try:
                        data = parse_ohlcv(data[0], mls)
                        await self.ws.send_json(data)
                        # logging.info(f"Send OHLCV: Sending {data}")
                    except Exception as exc:
                        logging.warning(f"EXCEPTION!! {exc}")

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
   
    async def _stopsend_ohlcv(
            self, exchange: str, base_id: str, quote_id: str, interval: str
        ) -> None:
        '''
        Private coroutine that stops serving OHLCV
            for `exchange`, `base_id`, quote_id`, `interval`
        '''
        
        if interval not in OHLCV_INTERVALS:
            await self.ws.send_json({
                'detail': "interval must be in the determined list"
            })
            return
        serving_id = f'ohlcv_{exchange}_{base_id}_{quote_id}_{interval}'
        self.serving_ids.remove(serving_id)

    def send_ohlcv(
            self,
            exchange: str,
            base_id: str,
            quote_id: str,
            interval: str,
            mls: bool = True
        ) -> None:
        '''
        Websocket API for sending OHLCV
        '''

        asyncio.run_coroutine_threadsafe(
            self._send_ohlcv(
                exchange, base_id, quote_id, interval, mls
            ),
            self.loop_handler.loop)

    def stopsend_ohlcv(
            self, exchange: str, base_id: str, quote_id: str, interval: str
        ) -> None:
        '''
        Websocket API for stopping sending OHLCV
        '''
        
        asyncio.run_coroutine_threadsafe(
            self._stopsend_ohlcv(
                exchange, base_id, quote_id, interval
            ),
            self.loop_handler.loop)
