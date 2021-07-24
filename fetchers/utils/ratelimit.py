import redis
import asyncio
from redis.exceptions import LockError
from common.config.constants import REDIS_HOST, REDIS_PASSWORD
from fetchers.config.constants import REST_RATE_LIMIT_REDIS_KEY


class GCRARateLimiter:
    '''
    Client-side request rate-limiter using the GCRA algorithm with Redis

    Applicable for multiple instances of a requesting object (e.g., a fetcher)
        sharing the same Redis rate-limiter key
    '''
    
    def __init__(
        self,
        exchange_name: str,
        rate_limit: int,
        period: float,
        redis_client: redis.Redis = None
    ):
        if not redis_client:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                username="default",
                password=REDIS_PASSWORD,
                decode_responses=True
            )
        self.redis_client = redis_client
        self.key = REST_RATE_LIMIT_REDIS_KEY.format(
            exchange = exchange_name
        )
        self.rate_limit = rate_limit
        self.period = period
        self.increment = self.period / self.rate_limit
   
    async def _is_limited(self):
        '''
        Different version

        Checks if the requesting function is rate-limited

        Source: https://dev.to/astagi/rate-limiting-using-python-and-redis-58gk
        
        See GCRA explanation: https://blog.ian.stapletoncordas.co/2018/12/understanding-generic-cell-rate-limiting.html
        '''

        t = self.redis_client.time()[0]
        try:
            with self.redis_client.lock(
                f'lock:{self.key}',
                blocking_timeout=self.increment
            ) as lock:
                self.redis_client.setnx(self.key, t)
                tat = max(float(self.redis_client.get(self.key)), t)
                allowed_at = tat + self.increment - self.period
                if t >= allowed_at:
                    new_tat = tat + self.increment
                    self.redis_client.set(self.key, new_tat)
                    return (False, None)
                return (True, allowed_at - t)
        except LockError:
            return (True, self.increment)

    async def wait(self):
        '''
        API to wait until the requesting function is not rate-limited
        '''
        
        while True:
            blocked, retry_after = await self._is_limited()
            if not blocked:
                break
            # print(f"GCRARateLimiter: Sleeping for {round(retry_after, 2)} seconds")
            await asyncio.sleep(retry_after)

    # async def _is_limited(self):
    # t = self.redis_client.time()[0]
    # self.redis_client.setnx(self.key, 0)
    # try:
    #     with self.redis_client.lock(
    #         f'lock:{self.key}',
    #         blocking_timeout=self.period
    #     ) as lock:
    #         tat = max(float(self.redis_client.get(self.key)), t)
    #         allowed_at = tat + self.increment - self.period
    #         if t >= allowed_at:
    #             new_tat = max(tat, t) + self.increment
    #             self.redis_client.set(self.key, new_tat)
    #             return (False, None)
    #         return (True, allowed_at - t)
    # except LockError:
    #     return (True, self.period)
