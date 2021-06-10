# This module contains constants

# Postgres
DBCONNECTION = "dbname=postgres user=postgres password=horus123 host=localhost port=15432"
REDIS_HOST = "localhost"

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
REDIS_PORT = 6379
REDIS_DELIMITER = ";;"
OHLCVS_BITTREX_REDIS_KEY = "ohlcvs_bittrex" # This will be a list in Redis
OHLCVS_BITFINEX_REDIS_KEY = "ohlcvs_bitfinex"