# Backend database session

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from dogpile.cache.region import make_region
from common.config.constants import (
    HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB,
    REDIS_HOST, REDIS_PASSWORD, REDIS_PORT
)
from web.routes.api.rest.utils import caching


# Connection and session
SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{HOST}/{POSTGRES_DB}"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL
    # connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Caching for REST requests
region = make_region().configure(
    'dogpile.cache.redis',
    arguments = {
        'host': REDIS_HOST,
        'port': REDIS_PORT,
        'password': REDIS_PASSWORD,
        'db': 0,
        'redis_expiration_time': 60*60*2,   # 2 hours
        'distributed_lock': True,
        'thread_local_lock': False
        }
)
regions = {'default': region}
cache = caching.ORMCache(regions)
cache.listen_on_session(scoped_session(SessionLocal))
