from sqlalchemy import (Column, DateTime, ForeignKeyConstraint, Index, Numeric,
    SmallInteger, String, Table, Text)
from sqlalchemy.orm import relationship
from common.config.constants import (OHLCVS_TABLE,
    OHLCVS_ERRORS_TABLE, SYMBOL_EXCHANGE_TABLE)
from web.database import Base, metadata


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

class SymbolExchange(Base):
    __tablename__ = SYMBOL_EXCHANGE_TABLE

    exchange = Column(String(100), primary_key=True, nullable=False, index=True)
    base_id = Column(String(20), primary_key=True, nullable=False, index=True)
    quote_id = Column(String(20), primary_key=True, nullable=False, index=True)
    symbol = Column(String(40), nullable=False)

class Ohlcv(Base):
    __tablename__ = OHLCVS_TABLE
    __table_args__ = (
        ForeignKeyConstraint(['exchange', 'base_id', 'quote_id'], ['symbol_exchange.exchange', 'symbol_exchange.base_id', 'symbol_exchange.quote_id'], ondelete='CASCADE'),
        Index('ohlcvs_exch_time_idx', 'exchange', 'time'),
        Index('ohlcvs_base_quote_time_idx', 'base_id', 'quote_id', 'time')
    )

    time = Column(DateTime(True), primary_key=True, nullable=False, index=True)
    exchange = Column(String(100), primary_key=True, nullable=False)
    base_id = Column(String(20), primary_key=True, nullable=False)
    quote_id = Column(String(20), primary_key=True, nullable=False)
    opening_price = Column(Numeric)
    highest_price = Column(Numeric)
    lowest_price = Column(Numeric)
    closing_price = Column(Numeric)
    volume = Column(Numeric)

    symbol_exchange = relationship('SymbolExchange')

t_common_basequote_30 = Table(
    'common_basequote_30', metadata,
    Column('base_id', String(20)),
    Column('quote_id', String(20))
)

t_ohlcvs_summary_daily = Table(
    'ohlcvs_summary_daily', metadata,
    Column('bucket', DateTime(True)),
    Column('exchange', String(100)),
    Column('base_id', String(20)),
    Column('quote_id', String(20)),
    Column('open', Numeric),
    Column('high', Numeric),
    Column('low', Numeric),
    Column('close', Numeric),
    Column('volume', Numeric)
)