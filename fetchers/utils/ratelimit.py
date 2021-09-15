import redis
import asyncio
import random
import time
from redis.exceptions import LockError
from common.config.constants import REDIS_HOST, REDIS_USER, REDIS_PASSWORD
from common.helpers.datetimehelpers import microseconds_to_seconds
from fetchers.config.constants import REST_RATE_LIMIT_REDIS_KEY


LOCK_TIMEOUT_SECS = 5


class GCRARateLimiter:
    '''
    Client-side request rate-limiter using the GCRA algorithm with Redis

    Applicable for multiple instances of a requesting object (e.g., a fetcher)
        sharing the same Redis rate-limiter key

    See GCRA explanation: https://blog.ian.stapletoncordas.co/2018/12/understanding-generic-cell-rate-limiting.html
    '''
    
    def __init__(
        self,
        rate_limit_key: str,
        rate_limit: float,
        period: float,
        redis_client: redis.Redis = None
    ):
        '''
        :params:
            `rate_limit_key`: unique key for this rate limiter
            `rate_limit`:
            `period`:
            `redis_client`:
        '''
    
        if not redis_client:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                username=REDIS_USER,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
        self.redis_client = redis_client
        self.key = rate_limit_key
        self.rate_limit = rate_limit
        self.period = period
        self.increment = self.period / self.rate_limit
   
    def _is_limited(self):
        '''
        Checks if the requesting function is rate-limited

        Source: https://dev.to/astagi/rate-limiting-using-python-and-redis-58gk
        '''

        secs, mics = self.redis_client.time()
        t = int(secs) + microseconds_to_seconds(float(mics))
        try:
            with self.redis_client.lock(
                f'lock:{self.key}',
                timeout=LOCK_TIMEOUT_SECS,
                blocking_timeout=0.01
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
        except Exception as exc:
            print(f"GCRARateLimiter: EXCEPTION: {exc}")

    async def wait(self):
        '''
        API call to wait until the requesting function is not rate-limited
        '''

        while True:
            limited, retry_after = self._is_limited()
            if not limited:
                break
            await asyncio.sleep(retry_after)
        
    async def __aenter__(self):
        await self.wait()

    async def __aexit__(self, exc_type, exc, tb):
        pass

class AsyncThrottler:
    '''
    An asyncio throttler using Redis

    Based on: https://github.com/hallazzang/asyncio-throttle
    '''
    
    def __init__(
        self,
        rate_limit_key: str,
        rate_limit: int,
        period: float,
        retry_interval: float = 0.01,
        redis_client: redis.Redis = None
    ):
        if not redis_client:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                username=REDIS_USER,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
        self.redis_client = redis_client
        self.key = rate_limit_key
        self.rate_limit = rate_limit
        self.period = period
        self.increment = period / rate_limit
        self.retry_interval = retry_interval

    def flush(self):
        secs, mics = self.redis_client.time()
        now = int(secs) + microseconds_to_seconds(float(mics))
        try:
            with self.redis_client.lock(
                f'lock:{self.key}',
                timeout=LOCK_TIMEOUT_SECS,
                blocking_timeout=0.01
            ) as lock:
                while int(self.redis_client.llen(self.key)) > 0:
                    if now - float(self.redis_client.lindex(self.key, 0)) > self.period:
                        self.redis_client.lpop(self.key)
                    else:
                        break
        except Exception:
            pass

    async def acquire(self):
        while True:
            self.flush()
            if int(self.redis_client.llen(self.key)) < self.rate_limit:
                break
            await asyncio.sleep(self.retry_interval)
        
        secs, mics = self.redis_client.time()
        now = int(secs) + microseconds_to_seconds(float(mics))
        self.redis_client.rpush(self.key, now)

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        pass
