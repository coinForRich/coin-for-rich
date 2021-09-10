#!/bin/sh
docker exec coin-timescaledb psql postgresql://postgres:icecreamyummy@localhost:5432/postgres -a -f /var/lib/postgresql/data/_user_scripts/cron/daily.sql
