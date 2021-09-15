# Dependencies for backend API

import redis
from typing import Generator
from web.db.session import SessionLocal
from common.config.constants import REDIS_HOST, REDIS_USER, REDIS_PASSWORD


def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_redis() -> Generator:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        username=REDIS_USER,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    try:
        yield redis_client
    finally:
        redis_client.close()
