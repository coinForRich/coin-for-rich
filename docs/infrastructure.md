**This file attempts to briefly describe how all the pieces and services are connected and glued together (at a high level)**

# How the services are connected
Assuming running with Docker Compose, at start up, [the websocket fetcher script](../scripts/fetchers/ws.py) establishes connections to the exchanges' websocket API endpoints. When real-time price data arrives from the websockets, hashes are created in the Redis server to store it. The Redis server is also used to create queues (in the form of sets) to fetch historical price data. At the same time, the Timescaledb container initializes the database based on [an init script](../scripts/database/init/create.sql) (which creates the Timescale extension, tables, aggregated views, etc.). Because the user-facing web service also starts at the same time, as soon as price data comes into Redis, the user can already view real-time prices. In the background, [a separate "updater" object](../fetchers/ws/updater.py) (the running of this object is embedded in [the websocket fetcher script](../scripts/fetchers/ws.py)) periodically collects real-time price data stored in Redis and bulk-insert it into Timescaledb.

To enable the user to download historical price data, three Celery workers are created using the running Redis server as the message broker. If the user requests historical data from an exchange, the corresponding Celery worker will receive a task to fetch such data from the exchange. As mentioned, all relevant information under the hood (e.g., exchange, symbols, time range, what are to fetch/being fetched) are managed using Redis `set` data structure (see [this doc](fetchers.md) for more details). During the fetching process, collected historical data is continuously bulk-inserted directly into Timescaledb. When the user wants to start populating aggregated views with actual data, they can run [this script](../scripts/database/once/populate_agg.sql). When all aggregated views are populated, the user can view price charts in different time windows (e.g., 6h, 1D, 3D, etc.).

# How the app Docker image is built
- The app Docker image is based on the Python 3.8.11 slim-buster image.
- Essential packages are then installed (e.g., wget, netcat, gnupg2, lsb-release, gcc, libpq-dev, procps, cron, tmux, redis-tools, postgresql-client-13).
- The directories needed to run the image are copied into the image.
- Required Python packages are installed, the log directory is created, the cron service is started, running privileges are enabled for the [init.sh script](../scripts/docker/init.sh) and the `wait-for` script.

# What the [init.sh](../scripts/docker/init.sh) script does
- Checks if Timescaledb/Postgres service is ready
- Checks if Redis service is ready
- Creates Celery workers and a log directory for Celery, and waits for those workers to be ready
- Creates a log directory for websocket fetchers, tmux sessions to view the following:
    - Timescaledb/Postgres
    - Redis
    - Fetchers
    - Web
    - Celery
- Creates a cron job for the [daily SQL task](../scripts/database/cron/daily.sql)
