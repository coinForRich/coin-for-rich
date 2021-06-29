# Helpers for asyncio including backoff, etc.

import asyncio
import backoff
import traceback
from fetchers.config.constants import THROTTLER_RATE_LIMITS, ASYNC_SIGNALS


# Backoff event handlers for httpx
def onbackoff(details):
    '''
    handler for backoff - on backoff event
    '''

    # Minimum throttler rate limit is 1
    throttler = details['kwargs']['throttler']
    throttler.period *= 2

    print(f"Reducing throttler rate limit to: 1 request per {round(throttler.period, 2)} seconds")

def onsuccessgiveup(details):
    '''
    handler for backoff - on success or giveup event
    '''

    throttler = details['kwargs']['throttler']
    exchange_name = details['kwargs']['exchange_name']
    throttler.period = \
        THROTTLER_RATE_LIMITS['RATE_LIMIT_SECS_PER_MIN'] / \
        THROTTLER_RATE_LIMITS['RATE_LIMIT_HITS_PER_MIN'][exchange_name]
    print(f"Setting throttler rate limit to: 1 request per {round(throttler.period, 2)} seconds")

# Asyncio exception handler for async tasks
def aio_handle_exception(loop, context):
    '''
    Asyncio exception handler
    '''

    # context["message"] will always be there;
    # but context["exception"] may not
    msg = context.get("exception", context["message"])
    print(f"Caught exception: {msg}")
    # print("Printing traceback...")
    # traceback.print_stack()
    print("Shutting down...")
    asyncio.create_task(aio_shutdown(loop))

# Asyncio shutdown function for async tasks
async def aio_shutdown(loop, signal=None):
    '''
    Cleans tasks tied to the service's shutdown
    '''

    if signal:
        print(f"Received exit signal {signal.name}...")
    print("Nacking outstanding messages")
    tasks = [
        t for t in asyncio.all_tasks() if t \
            is not asyncio.current_task()
    ]

    [task.cancel() for task in tasks]

    print(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"Flushing metrics")
    loop.stop()

# Asyncio set exception handler for a loop
def aio_set_exception_handler(loop):
    '''
    Sets exception handler for a loop
    '''

    for s in ASYNC_SIGNALS:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(aio_shutdown(loop, signal=s))
        )
    loop.set_exception_handler(aio_handle_exception)