# Models for OHLCV

from sqlalchemy import (
    Column, DateTime, ForeignKeyConstraint,
    Index, Numeric, String, Table
)
from sqlalchemy.orm import relationship
from common.config.constants import OHLCVS_TABLE
from web.db.base import Base, metadata


class Ohlcv(Base):
    __tablename__ = OHLCVS_TABLE
    __table_args__ = (
        ForeignKeyConstraint(['exchange', 'base_id', 'quote_id'], ['symbol_exchange.exchange', 'symbol_exchange.base_id', 'symbol_exchange.quote_id'], ondelete='CASCADE'),
        Index('ohlcvs_time_idx', 'time'),
        Index('ohlcvs_exch_time_idx', 'exchange', 'time'),
        Index('ohlcvs_base_quote_time_idx', 'base_id', 'quote_id', 'time')
    )

    time = Column(DateTime(True), primary_key=True, nullable=False, index=True)
    exchange = Column(String(100), primary_key=True, nullable=False)
    base_id = Column(String(20), primary_key=True, nullable=False)
    quote_id = Column(String(20), primary_key=True, nullable=False)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)

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

t_ohlcvs_summary_5min = Table(
    'ohlcvs_summary_5min', metadata,
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

t_ohlcvs_summary_15min = Table(
    'ohlcvs_summary_15min', metadata,
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

t_ohlcvs_summary_30min = Table(
    'ohlcvs_summary_30min', metadata,
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

t_ohlcvs_summary_1hour = Table(
    'ohlcvs_summary_1hour', metadata,
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

t_ohlcvs_summary_6hour = Table(
    'ohlcvs_summary_6hour', metadata,
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

t_ohlcvs_summary_12hour = Table(
    'ohlcvs_summary_12hour', metadata,
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

t_ohlcvs_summary_7day = Table(
    'ohlcvs_summary_7day', metadata,
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
