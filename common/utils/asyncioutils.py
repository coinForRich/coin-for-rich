# Utils for asyncio that may be used in any module

import traceback
import asyncio
from signal import Signals
from threading import Thread
from typing import Any
from fetchers.config.constants import ASYNC_SIGNALS


def aio_handle_exception(loop: asyncio.AbstractEventLoop, context: Any) -> None:
    '''
    Asyncio exception handler, for use with
        `aio_shutdown` and `aio_set_exception_handler`

    :params:
        `loop`: asyncio event loop
        `context`: ..?
    
    Source: https://www.roguelynn.com/words/asyncio-exception-handling/
    '''

    # context["message"] will always be there;
    # but context["exception"] may not
    msg = context.get("exception", context["message"])
    print(f"Caught exception: {msg}")
    print("Printing traceback...")
    traceback.print_stack()
    print("Shutting down...")
    asyncio.create_task(aio_shutdown(loop))

async def aio_shutdown(loop: asyncio.AbstractEventLoop, signal: Signals=None) -> None:
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

def aio_set_exception_handler(loop: asyncio.AbstractEventLoop) -> None:
    '''
    Sets exception handler for a loop

    Source: https://www.roguelynn.com/words/asyncio-exception-handling/
    '''

    for s in ASYNC_SIGNALS:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(aio_shutdown(loop, signal=s))
        )
    loop.set_exception_handler(aio_handle_exception)


class AsyncLoopThread(Thread):
    '''
    Async loop thread for adding coroutines to
        a different OS thread

    Source: https://stackoverflow.com/questions/34499400/how-to-add-a-coroutine-to-a-running-asyncio-loop
    '''

    def __init__(self, daemon: bool = True):
        super().__init__(daemon=daemon)
        self.loop = asyncio.new_event_loop()

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
