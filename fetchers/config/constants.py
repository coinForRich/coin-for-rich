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
    'bittrex': 100, # increased from 55
    'bitfinex': 100, # increased from 85
    'binance': 200 # decreased from 500
}
HTTPX_DEFAULT_TIMEOUT = 3.0
HTTPX_DEFAULT_RETRIES = 12

# Asyncio signals
ASYNC_SIGNALS = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)

# REST/WS rate limit Redis keys
REST_RATE_LIMIT_REDIS_KEY = "rest_rate_limit_{exchange}"
WS_RATE_LIMIT_REDIS_KEY = "ws_rate_limit_{exchange}"

# Websocket Redis keys
# Sub is for storing temp subscribed ws data to update psql db later
# Serve is for serving real time data to our web service
WS_SUB_PREFIX = "ws_sub_"
WS_SUB_REDIS_KEY = "ws_sub_{exchange}{delimiter}{base_id}{delimiter}{quote_id}"
WS_SERVE_REDIS_KEY = "ws_serve_{exchange}{delimiter}{base_id}{delimiter}{quote_id}"
WS_SUB_LIST_REDIS_KEY = "ws_sub_list"
WS_SUB_PROCESSING_REDIS_KEY = "ws_sub_processing"

# To-fetch and fetching Redis set keys
OHLCVS_TOFETCH_REDIS_KEY = "ohlcvs_tofetch_{exchange}"
OHLCVS_FETCHING_REDIS_KEY = "ohlcvs_fetching_{exchange}"

# PSQL Constants
OHLCV_UNIQUE_COLUMNS = ("time", "exchange", "base_id", "quote_id")
OHLCV_UPDATE_COLUMNS = ("open", "high", "low", "close", "volume")
SYMEXCH_UNIQUE_COLUMNS = ("exchange", "base_id", "quote_id")
SYMEXCH_UPDATE_COLUMNS = ("is_trading", "symbol")
NUM_DECIMALS = 4 # number of decimals
