# Base for all REST fetchers

import time
import redis
import asyncio
from common.config.constants import REDIS_HOST, REDIS_PASSWORD
from fetchers.config.constants import REST_RATE_LIMIT_REDIS_KEY, THROTTLER_RATE_LIMITS
from fetchers.utils.asyncioutils import aio_set_exception_handler


class BaseOHLCVFetcher:
    '''
    Base REST fetcher for all exchanges
    '''

    def __init__(self):
        pass
    
    async def resume_fetch(self, update: bool=False):
        '''
        Resumes fetching tasks if there're params inside Redis sets
        '''

        # Asyncio gather 1 task:
        # - Consume from Redis to-fetch
        await asyncio.gather(
            self.consume_ohlcvs_redis(update)
        )

    def run_resume_fetch(self):
        '''
        Runs the resuming of fetching tasks
        '''

        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
        aio_set_exception_handler(loop)
        try:
            print("Run_resume_fetch: Resuming fetching tasks from Redis sets")
            loop.run_until_complete(self.resume_fetch())
        finally:
            print("Run_resume_fetch: Finished fetching OHLCVS")
            loop.close()
