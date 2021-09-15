# Utils for asyncio including backoffs used in fetchers

from typing import Any
from fetchers.config.constants import THROTTLER_RATE_LIMITS


# Backoff event handlers for httpx
def onbackoff(details: Any):
    '''
    handler for backoff - on backoff event
    '''

    # Minimum throttler rate limit is 1
    throttler = details['kwargs']['throttler']
    throttler.period *= 2

    # print(f"Reducing throttler rate limit to: 1 request per {round(throttler.period, 2)} seconds")

def onsuccessgiveup(details: Any):
    '''
    handler for backoff - on success or giveup event
    '''

    throttler = details['kwargs']['throttler']
    exchange_name = details['kwargs']['exchange_name']
    throttler.period = \
        THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN'] / \
        THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN'][exchange_name]
    # print(f"Setting throttler rate limit to: 1 request per {round(throttler.period, 2)} seconds")
