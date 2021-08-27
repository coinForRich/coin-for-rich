# This module contains constants for all apps

from dotenv import dotenv_values 

# Load env vars
configs = dotenv_values(".env")

# Common host
HOST = configs.get('COMMON_HOST')

# Postgres
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = configs.get('POSTGRES_PASSWORD')
POSTGRES_DB = "postgres"
POSTGRES_PORT = 5432
DBCONNECTION = f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={HOST} port={POSTGRES_PORT}"
OHLCVS_TABLE = "ohlcvs"
OHLCVS_ERRORS_TABLE = "ohlcvs_errors"
SYMBOL_EXCHANGE_TABLE = "symbol_exchange"

# Redis common vars
REDIS_HOST = HOST
REDIS_USER = "default"
REDIS_PORT = 6379
REDIS_PASSWORD = configs.get('REDIS_PASSWORD')
REDIS_DELIMITER = ";;"

# Celery
CELERY_REDIS_URL = f"redis://default:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0"

# Default datetime string format when dealing with PSQL
DEFAULT_DATETIME_STR_QUERY = "%Y-%m-%dT%H:%M:%S"
DEFAULT_DATETIME_STR_RESULT = "%Y-%m-%dT%H:%M:%S%z"