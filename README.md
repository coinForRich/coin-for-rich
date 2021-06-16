# README

## How to use
It's best to use services in Docker
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

### Run them all together**

## Best practices learned
### SSH keys and logins
Automate SSH logins: https://mindchasers.com/dev/ssh-automate-login
### Git
Python .gitignore file config example: https://github.com/github/gitignore/blob/master/Python.gitignore
### Http requests and rate limits
Limit rate of requests: https://pypi.org/project/asyncio-throttle/

Limit rate of requests and number of concurrent requests: https://medium.com/analytics-vidhya/async-python-client-rate-limiter-911d7982526b
### Postgresql
Postgresql meta commands: https://dataschool.com/learn-sql/meta-commands-in-psql/

Populating postgres tables: https://www.postgresql.org/docs/current/populate.html

Postgresql table populating methods, benchmarked (*spoiler: copy is best*): https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
### **Celery**
Stop Celery worker processes: https://stackoverflow.com/questions/29306337/how-to-stop-celery-worker-process/48462005
### **Caching Using Redis**
Caching using Redis, from Redis Lab: https://redislabs.com/blog/query-caching-redis/
Postgresql and Redis caching, an experiment: https://medium.com/wultra-blog/achieving-high-performance-with-postgresql-and-redis-deddb7012b16
Import CSV data into Redis: https://daten-und-bass.io/blog/import-csv-data-into-redis-with-a-single-cli-command/