# Tables models for analytic views created in Postgres db

from sqlalchemy import (
    Column, DateTime, Index,
    Numeric, String, Table, BigInteger
)
from web.db.base import metadata

# Geometric average daily return
geo_daily_return = Table(
    'geo_daily_return', metadata,
    Column('exchange', String(100)),
    Column('base_id', String(20)),
    Column('quote_id', String(20)),
    Column('daily_return_pct', Numeric),
    Index('geo_dr_idx', 'exchange', 'base_id', 'quote_id')
)

# Top 10 hot commodities(bases)
top_20_quoted_vol = Table(
    'top_20_quoted_vol', metadata,
    Column('bqgrp', String()),
    Column('total_volume', Numeric),
    Index('top_20_qvlm_idx', 'bqgrp')
)

# Weekly return
weekly_return = Table(
    'weekly_return', metadata,
    Column('time', DateTime(True)),
    Column('exchange', String(100)),
    Column('base_id', String(20)),
    Column('quote_id', String(20)),
    Column('weekly_return_pct', Numeric),
    Index('wr_idx', 'exchange', 'base_id', 'quote_id', 'time')
)
