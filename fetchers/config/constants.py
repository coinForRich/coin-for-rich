# This module contains constants

DBCONNECTION = "dbname=postgres user=postgres password=horus123 host=localhost port=15432"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

THROTTLER_RATE_LIMITS = {
    'RATE_LIMIT_HITS_PER_MIN': {
        'bittrex': 40,
        'bitfinex': 70
    },
    'RATE_LIMIT_SECS_PER_MIN': 60
}

HTTPX_MAX_CONCURRENT_CONNECTIONS = 32
