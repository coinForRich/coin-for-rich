# This module contains constants

import signal
from dotenv import dotenv_values 

# Load env vars
configs = dotenv_values(".env")

# HTTPX/REST
THROTTLER_RATE_LIMITS = {
    'RATE_LIMIT_HITS_PER_MIN': {
        'bittrex': 50,
        'bitfinex': 80
    },
    'RATE_LIMIT_SECS_PER_MIN': 60
}
HTTPX_MAX_CONCURRENT_CONNECTIONS = 32

# Asyncio signals
ASYNC_SIGNALS = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
