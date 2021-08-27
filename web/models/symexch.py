from sqlalchemy import (
    Column, Index, String, Text
)
from sqlalchemy.orm import relationship
from common.config.constants import (
    OHLCVS_TABLE, OHLCVS_ERRORS_TABLE, SYMBOL_EXCHANGE_TABLE
)
from web.db.base import Base, metadata


class SymbolExchange(Base):
    __tablename__ = SYMBOL_EXCHANGE_TABLE
    __table_args__ = (
        Index('symexch_exch_sym_idx', 'exchange', 'symbol', unique=True),
        Index('symexch_exch_idx', 'exchange'),
        Index('symexch_base_idx', 'base_id'),
        Index('symexch_quote_idx', 'quote_id')
    )

    exchange = Column(String(100), primary_key=True, nullable=False, index=True)
    base_id = Column(String(20), primary_key=True, nullable=False, index=True)
    quote_id = Column(String(20), primary_key=True, nullable=False, index=True)
    symbol = Column(String(40), nullable=False)
