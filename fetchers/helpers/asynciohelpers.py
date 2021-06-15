# Helpers for asyncio including backoff, etc.

import backoff
from fetchers.config.constants import THROTTLER_RATE_LIMITS


# Backoff event handlers
def onbackoff(details):
    '''
    handler for backoff - on backoff event
    '''

    throttler = details['kwargs']['throttler']
    throttler.rate_limit -= 1
    print(f"Reducing throttler rate limit to {throttler.rate_limit}")

def onsuccessgiveup(details):
    '''
    handler for backoff - on success or giveup event
    '''

    throttler = details['kwargs']['throttler']
    exchange_name = details['kwargs']['exchange_name']
    throttler.rate_limit = THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN'][exchange_name]
    print(f"Setting throttler rate limit to {throttler.rate_limit}")
