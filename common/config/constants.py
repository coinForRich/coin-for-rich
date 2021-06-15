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

# Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DELIMITER = ";;"