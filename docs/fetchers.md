**This file briefly documents how fetchers work**
# REST fetchers
The fetching process of a REST fetcher can be broken down into the following:
- Initialize parameters (exchange, symbols, time range, etc.) to fetch into a Redis queue (the Redis queue can be a set)
- Consume parameters from the Redis queue
- When there are more timestamps in the time range to fetch, generate more parameters based on the exchange's constraints and feed them into the Redis queue

# Websocket fetchers
The fetching process of a Websocket fetcher can be broken down into the following:
- Initialize a websocket connection to the exchange's API
- For each exchange-base-quote combination, create two similar Redis hashes that contain timestamp and price information as keys (or fields, as per [Redis](https://redis.io/docs/data-types/hashes/))
    - One for bulk-insert into Timescale/PSQL
    - One for serving the app's web API (real-time chart)
- Update the information in the hash whenever the data timestamp from the exchange is newer
- A separate script periodically collects the data from all the hashes and bulk insert them into Timescale/PSQL
