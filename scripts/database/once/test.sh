#!/bin/sh
pg_isready -d postgres -h coin-timescaledb -p 5432 -U postgres
psql postgresql://postgres:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/postgres -a -f /coin-for-rich/scripts/database/once/test.sql
