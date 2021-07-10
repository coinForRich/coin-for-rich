# This module contains constants

import signal
from dotenv import dotenv_values 

# Load env vars
configs = dotenv_values(".env")

# HTTPX/REST
THROTTLER_RATE_LIMITS = {
    'RATE_LIMIT_HITS_PER_MIN': {
        'bittrex': 55,
        'bitfinex': 85,
        'binance': 1200
    },
    'RATE_LIMIT_SECS_PER_MIN': 60
}
HTTPX_MAX_CONCURRENT_CONNECTIONS = {
    'bittrex': 55,
    'bitfinex': 85,
    'binance': 32
}

# Asyncio signals
ASYNC_SIGNALS = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)

# Websocket Redis keys
# Sub is for storing temp subscribed ws data to update psql db later
# Serve is for serving real time data to our web service
WS_SUB_PREFIX = "ws_sub_"
WS_SUB_REDIS_KEY = "ws_sub_{exchange}{delimiter}{base_id}{delimiter}{quote_id}"
WS_SERVE_REDIS_KEY = "ws_serve_{exchange}{delimiter}{base_id}{delimiter}{quote_id}"
WS_SUB_LIST_REDIS_KEY = "ws_sub_list"
WS_SUB_PROCESSING_REDIS_KEY = "ws_sub_processing"