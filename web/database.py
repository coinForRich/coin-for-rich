from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from common.config.constants import (
    HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
)


SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{HOST}/{POSTGRES_DB}"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL
    # connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
metadata = Base.metadata
