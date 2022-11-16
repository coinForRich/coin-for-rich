# This module contains constants for all apps

import os
from dotenv import dotenv_values 

# Load env vars
configs = dotenv_values(".env")

# Common host (for fallback)
COMMON_HOST = configs.get('COMMON_HOST')

# Postgres
POSTGRES_HOST = os.getenv('POSTGRES_HOST') or COMMON_HOST
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD') or configs.get('POSTGRES_PASSWORD')
POSTGRES_USER = "postgres"
POSTGRES_DB = "postgres"
POSTGRES_PORT = os.getenv('POSTGRES_PORT') or configs.get('POSTGRES_PORT')
DBCONNECTION = f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST} port={POSTGRES_PORT}"
OHLCVS_TABLE = "ohlcvs"
OHLCVS_ERRORS_TABLE = "ohlcvs_errors"
SYMBOL_EXCHANGE_TABLE = "symbol_exchange"

# Redis common vars
REDIS_HOST = os.getenv('REDIS_HOST') or COMMON_HOST
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD') or configs.get('REDIS_PASSWORD')
REDIS_USER = "default"
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_DELIMITER = ";;"

# Celery
CELERY_REDIS_URL = f"redis://{REDIS_USER}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/0"

# Default datetime string format when dealing with PSQL
DEFAULT_DATETIME_STR_QUERY = "%Y-%m-%dT%H:%M:%S"
DEFAULT_DATETIME_STR_RESULT = "%Y-%m-%dT%H:%M:%S%z"
