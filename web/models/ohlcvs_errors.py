from sqlalchemy import (
    Column, DateTime, ForeignKeyConstraint, Index, Numeric,
    SmallInteger, String, Table, Text
)
from sqlalchemy.orm import relationship
from common.config.constants import (
    OHLCVS_TABLE, OHLCVS_ERRORS_TABLE, SYMBOL_EXCHANGE_TABLE
)
from web.db.base import Base, metadata


class OhlcvsError(Base):
    __tablename__ = OHLCVS_ERRORS_TABLE

    exchange = Column(String(100), primary_key=True, nullable=False)
    symbol = Column(String(20), primary_key=True, nullable=False)
    start_date = Column(DateTime(True), primary_key=True, nullable=False)
    end_date = Column(DateTime(True), primary_key=True, nullable=False)
    time_frame = Column(String(10), primary_key=True, nullable=False)
    ohlcv_section = Column(String(30))
    resp_status_code = Column(SmallInteger)
    exception_class = Column(Text, primary_key=True, nullable=False)
    exception_message = Column(Text)
