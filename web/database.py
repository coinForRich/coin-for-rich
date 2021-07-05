from sqlalchemy.ext.declarative import declarative_base
from common.config.constants import POSTGRES_PASSWORD


SQL_ALCHEMY_DATABASE_URL = f"postgresql://postgres:{POSTGRES_PASSWORD}@localhost/postgres"
Base = declarative_base()
metadata = Base.metadata
