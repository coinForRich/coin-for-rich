#!/bin/sh

# Postgres/timescaledb
pg_isready -h $POSTGRES_HOST -p 5432 -U postgres -d postgres
status_code=$(($?))
if [ $status_code -eq 0 ];
then
    echo "Timescaledb is ready!"
    echo "- Timescaledb/Postgresql is ready to connect" >> logs/init.log
else
    echo "- Cannot connect to Timescaledb/Postgresql" >> logs/init.log
    exit $status_code
fi

# Redis
redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD ping
status_code=$(($?))
if [ $status_code -eq 0 ];
then
    echo "Redis is ready!"
    echo "- Redis is ready to connect" >> logs/init.log
else
    echo "- Cannot connect to Redis" >> logs/init.log
    exit $status_code
fi

# Celery
echo "Creating Celery workers..."
celery -A celery_app.celery_main worker -Q bitfinex_rest -n bitfinexRestWorker@h -l INFO --logfile="./logs/celery_main_%n_$(date +'%Y-%m-%dT%H:%M:%S').log" --detach

celery -A celery_app.celery_main worker -Q binance_rest -n binanceRestWorker@h -l INFO --logfile="./logs/celery_main_%n.log_$(date +'%Y-%m-%dT%H:%M:%S').log" --detach

celery -A celery_app.celery_main worker -Q bittrex_rest -n bittrexRestWorker@h -l INFO --logfile="./logs/celery_main_%n.log_$(date +'%Y-%m-%dT%H:%M:%S').log" --detach

while true; do
    status=$(celery -A celery_app.celery_main status | grep '3 nodes')
    if [ "$status" == "3 nodes online." ];
    then
        echo "All 3 Celery workers are online!"
        break
    else
        echo "Waiting for Celery workers to be online..."
    fi
done
echo "- Celery workers created" >> logs/init.log

# Tmux
echo "Creating tmux sessions..."
ses_psql="psql"
ses_redis="redis"
ses_fetch_rest="frest"
ses_fetch_ws="fws"
ses_web="web"

tmux new-session -d -s $ses_psql

tmux new-session -d -s $ses_redis

tmux new-session -d -s $ses_fetch_rest

tmux new-session -d -s $ses_fetch_ws \; \
    split-window -v \; \
    split-window -h -p 30 \; \
    split-window -h -p 50 \; \

tmux new-session -d -s $ses_web
echo "tmux sessions created!"
echo "- tmux sessions created" >> logs/init.log

# Cron
(crontab -l 2>/dev/null; echo "0 0 * * * psql postgresql://postgres:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/postgres -f /coin-for-rich/scripts/database/cron/daily.sql") | crontab -
echo "cron job added!"
echo "- cron job added" >> logs/init.log

# Print init result
echo "Initialization complete"
echo "- Initialization complete" >> logs/init.log

bash
