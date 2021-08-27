from sqlalchemy import (
    Column, Numeric, String
)
from sqlalchemy.orm import relationship
from web.db.base import Base, metadata


class TestTable(Base):
    __tablename__ = 'test'
    __test__ = False # Skip pytest class

    id = Column(Numeric, primary_key=True, nullable=False)
    b = Column(String(20), primary_key=True, nullable=False)
    q = Column(String(20), primary_key=True, nullable=False)
    o = Column(Numeric)
    c = Column(Numeric)
