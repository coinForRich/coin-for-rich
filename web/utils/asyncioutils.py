import asyncio
from threading import Thread


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

class WSBroadcaster:
    '''
    Websocket broadcaster for clients that subscribe to our web
    '''

    def __init__(self):
        self.loop_handler = AsyncLoopThread()
        self.loop_handler.start()

    async def _broadcast_from_redis_list(self, ws, redis_client, rkey):
        '''
        Coroutine that broadcasts from Redis list
        :params:
            `ws`: fastAPI WebSocket obj
            `redis_client`: Redis client
            `rkey`: Redis key
        '''
        
        while True:
            data = redis_client.lrange(rkey, -1, -1)
            if data:
                await ws.send_text(f"Data: {data} from key {rkey}")
            await asyncio.sleep(3)

    def broadcast_from_redis_list(self, ws, redis_client, rkey):
        '''
        Main broadcast function to use
        '''

        # self.loop_handler = AsyncLoopThread()
        # self.loop_handler.start()
        asyncio.run_coroutine_threadsafe(
            self._broadcast_from_redis_list(ws, redis_client, rkey),
            self.loop_handler.loop
        )