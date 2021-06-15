# This module contains constants

import os
import signal
from dotenv import load_dotenv 

# Load env vars
load_dotenv()

# Postgres
DBCONNECTION = f"dbname=postgres user=postgres password={os.getenv('POSTGRES_PASSWORD')} host=localhost port=5432"
OHLCVS_TABLE = "ohlcvs"
SYMBOL_EXCHANGE_TABLE = "symbol_exchange"

# HTTPX/REST
THROTTLER_RATE_LIMITS = {
    'RATE_LIMIT_HITS_PER_MIN': {
        'bittrex': 50,
        'bitfinex': 80
    },
    'RATE_LIMIT_SECS_PER_MIN': 60
}
HTTPX_MAX_CONCURRENT_CONNECTIONS = 32

# Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DELIMITER = ";;"
OHLCVS_BITTREX_TOFETCH_REDIS_KEY = "ohlcvs_bittrex_tofetch"   # This will be a set in Redis

# Asyncio signals
ASYNC_SIGNALS = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
