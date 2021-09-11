# README

## How to use
It's best to use Postgresql and Redis in Docker
### Create a `.env` file
In the root folder of the project, create a `.env` file with the following content:
```
POSTGRES_PASSWORD=yourPostgresPassword
REDIS_PASSWORD=yourRedisPassword
COMMON_HOST=yourLocalhost
```

In the `celery_app` folder, create a new folder named `beat`

In the root folder of the project, create a new folder with another folder inside so they are like this: `logs/celery`

### Install dependencies
The following are required to install psycopg2 (?)
```
sudo apt install gcc libpq-dev
```
### Setup a Postgresql/Timescaledb database
#### Run container
Remember to bind-mount data volume to make it persistent
```
docker run -d --name coin-timescaledb -p 5432:5432 -v /your/absolute/data/path/_postgresdata:/var/lib/postgresql/data -e POSTGRES_PASSWORD=yourPostgresPassword timescale/timescaledb:2.3.0-pg13
```
#### Copy cron SQL script (should be done with Dockerfile/Docker Compose?)
```
mkdir -p /your/absolute/data/path/_postgresdata/scripts/cron && cp ./cron_scripts/database/daily.sql $_
```
#### Configure crontab to run local cron script
```
...
```
### Setup a Redis server
Remember to bind-mount data volume to make it persistent
```
docker run -d --name coin-redis -p 6379:6379 -v /your/absolute/data/path/_redisdata:/data redis:6.2 redis-server --appendonly yes --requirepass "yourRedisPassword"
```
### Setup Celery
Celery should be installed along with other Python packages

### Run them all together
#### REST updaters
REST updaters are run with Celery to make sure data is as consistent with the exchanges as possible. It is to mitigate any disconnection with Websocket subscribers

To run REST updaters:
- Open 5 tmux terminals panes, activate your virtual environment if any
- In the first 4 terminals, run the following:
    ```
    celery -A celery_app.celery_main worker -Q bitfinex_rest -n bitfinexRestWorker@h -l INFO --logfile="./logs/celery_main_%n_$(date +'%Y-%m-%dT%H:%M:%S').log" --detach

    celery -A celery_app.celery_main worker -Q binance_rest -n binanceRestWorker@h -l INFO --logfile="./logs/celery_main_%n.log_$(date +'%Y-%m-%dT%H:%M:%S').log" --detach

    celery -A celery_app.celery_main worker -Q bittrex_rest -n bittrexRestWorker@h -l INFO --logfile="./logs/celery_main_%n.log_$(date +'%Y-%m-%dT%H:%M:%S').log" --detach

    celery -A celery_app.celery_main worker -Q all_rest -c 4 -n allRestWorker@h -l INFO --logfile="./logs/celery_main_%n.log_$(date +'%Y-%m-%dT%H:%M:%S').log" --detach
    ```
- In the last terminal (may not be needed):
    - You have to run the task called `all_fetch_symbol_data` first, so all symbols and exchanges are loaded into database
    - And then to schedule periodic updates, `celery -A celery_app.celery_main beat -s ./celery_app/beat/celery_beat`
- `python -m scripts.fetchers.rest fetch --exchange bitfinex --start 2021-01-01T00:00:00 --end 2021-01-02T00:00:00`

#### Websocket subscribers
Websocket subscribers and updater are run in different terminals

To run websocket subscribers and updater:
- Open 4 tmux terminals panes, activate your virtual environment if any
- In the first 3 terminals, run this command in each:
    - `python -m scripts.fetchers.ws fetch --exchange <exchange name>`
    - For example, `python -m commands.fetchws fetch --exchange bitfinex` for Bitfinex
- In the last terminal, run this command:
    - `python -m scripts.fetchers.ws update`

#### Run tests
At the project root folder, run:
`pytest`

#### Start web app server
`uvicorn web.main:app --reload`

#### After having some data
Inside psql tmux session:
`\i /coin-for-rich/scripts/database/once/populate_agg.sql`

## Explore
- Docs are stored in the `docs` folder
- Commands are stored in the `commands` folder

## Docker
### Database
`psql -U postgres -d postgres -h coin-psql -W`
