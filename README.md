# README

## How to use
It's best to use Postgresql and Redis in Docker
### Setup a Postgresql/Timescaledb database
Remember to bind-mount data volume to make it persistent
```
docker run -d --name coin-timescaledb -p 5432:5432 -v /root/coin_data/_postgresdata:/var/lib/postgresql/data -e POSTGRES_PASSWORD=your_password timescale/timescaledb:2.3.0-pg13
```
### Setup a Redis server
Remember to bind-mount data volume to make it persistent
```
docker run -d --name coin-redis -p 6379:6379 -v /root/coin_data/_redisdata:/data redis:6.2 redis-server --appendonly yes --requirepass "your_password"
```
### Setup Celery

### Run them all together
Websocket subscribers and updaters are run in different terminals

REST updaters are run with Celery

### Run tests
`python -m pytest`