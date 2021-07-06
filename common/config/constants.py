# This module contains constants

from dotenv import dotenv_values 

# Load env vars
configs = dotenv_values(".env")

# Common host
HOST = configs.get('COMMON_HOST')

# Postgres
POSTGRES_PASSWORD = configs.get('POSTGRES_PASSWORD')
DBCONNECTION = f"dbname=postgres user=postgres password={POSTGRES_PASSWORD} host={HOST} port=5432"
OHLCVS_TABLE = "ohlcvs"
OHLCVS_ERRORS_TABLE = "ohlcvs_errors"
SYMBOL_EXCHANGE_TABLE = "symbol_exchange"

# Redis common vars
REDIS_HOST = HOST
REDIS_PORT = 6379
REDIS_PASSWORD = configs.get('REDIS_PASSWORD')
REDIS_DELIMITER = ";;"

# Celery
CELERY_REDIS_URL = f"redis://default:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0"

# Default datetime string format
DEFAULT_DATETIME_STR_QUERY = "%Y-%m-%dT%H:%M:%S"
DEFAULT_DATETIME_STR_RESULT = "%Y-%m-%dT%H:%M:%S%z"