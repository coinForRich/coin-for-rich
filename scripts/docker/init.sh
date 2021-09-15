#!/bin/sh

# Postgres/timescaledb
pg_isready -h $POSTGRES_HOST -p 5432 -U postgres -d postgres
status_code=$(($?))
if [ $status_code -eq 0 ];
then
    echo "Timescaledb is ready!"
    echo "- $(date +'%Y-%m-%dT%H:%M:%S') - Timescaledb/Postgresql is ready to connect" >> logs/init.log
else
    echo "- $(date +'%Y-%m-%dT%H:%M:%S') - Cannot connect to Timescaledb/Postgresql" >> logs/init.log
    exit $status_code
fi

# Redis
redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD ping
status_code=$(($?))
if [ $status_code -eq 0 ];
then
    echo "Redis is ready!"
    echo "- $(date +'%Y-%m-%dT%H:%M:%S') - Redis is ready to connect" >> logs/init.log
else
    echo "- $(date +'%Y-%m-%dT%H:%M:%S') - Cannot connect to Redis" >> logs/init.log
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
echo "- $(date +'%Y-%m-%dT%H:%M:%S') - Celery workers created" >> logs/init.log

# Tmux sessions
echo "Creating tmux sessions..."
ses_psql="psql"
ses_redis="redis"
ses_fetch="fetch"
ses_web="web"
ses_celery="celery"

tmux new-session -d -s $ses_psql \; \
    send-keys 'psql postgresql://postgres:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/postgres' C-m

tmux new-session -d -s $ses_redis \; \
    send-keys 'redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD' C-m

tmux new-session -d -s $ses_fetch \; \
    split-window -v \; \
    split-window -h -p 66 \; \
    split-window -h -p 50 \; \
    select-pane -t 0 \; \
    split-window -h -p 50 \; \
    select-pane -t 1 \; send-keys 'python -m scripts.fetchers.ws update' C-m \; \
    select-pane -t 2 \; send-keys 'python -m scripts.fetchers.ws fetch --exchange bitfinex' C-m \; \
    select-pane -t 3 \; send-keys 'python -m scripts.fetchers.ws fetch --exchange binance' C-m \; \
    select-pane -t 4 \; send-keys 'python -m scripts.fetchers.ws fetch --exchange bittrex' C-m \; \
    select-pane -t 0 \; send-keys 'echo ">>> You can fetch data from an exchange REST API using a command like this: python -m scripts.fetchers.rest fetch --exchange bitfinex --start 2021-01-01T00:00:00 --end 2021-01-02T00:00:00"' C-m

tmux new-session -d -s $ses_web \; \
    send-keys 'uvicorn web.main:app --reload --host 0.0.0.0' C-m

tmux new-session -d -s $ses_celery \; \
    send-keys 'celery -A celery_app.celery_main flower --address=0.0.0.0 --port=5566' C-m

echo "tmux sessions created!"
echo "- $(date +'%Y-%m-%dT%H:%M:%S') - tmux sessions created" >> logs/init.log

# Cron
(crontab -l 2>/dev/null; echo "0 0 * * * psql postgresql://postgres:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/postgres -f /coin-for-rich/scripts/database/cron/daily.sql") | crontab -
echo "cron job added!"
echo "- $(date +'%Y-%m-%dT%H:%M:%S') - cron job added" >> logs/init.log

# Print init result
echo "Initialization complete"
echo "- $(date +'%Y-%m-%dT%H:%M:%S') - Initialization complete" >> logs/init.log

bash
