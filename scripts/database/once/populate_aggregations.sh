#!/bin/sh
psql postgresql://postgres:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/postgres -a -f /coin-for-rich/scripts/database/once/populate_aggregations.sql
