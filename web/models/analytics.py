# Tables models for analytic views created in Postgres db

from sqlalchemy import (
    Column, DateTime, ForeignKeyConstraint,
    Index, Numeric, String, Table, BigInteger
)
from sqlalchemy.orm import relationship
from web.db.base import metadata

top_500_daily_return = Table(
    'top_500_daily_return', metadata,
    Column('ranking', BigInteger),
    Column('exchange', String(100)),
    Column('base_id', String(20)),
    Column('quote_id', String(20)),
    Column('gavg_daily_return', Numeric),
    Index('top_500_dr_idx', 'exchange', 'base_id', 'quote_id')
)
