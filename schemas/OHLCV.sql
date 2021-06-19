-- For use with psycopg2
-- Before using the queries...
-- psql -h localhost -p 5432 -U postgres -W -d postgres
-- password is in ENV file

-- Create OHLCVS table
CREATE TABLE ohlcvs (
   time TIMESTAMPTZ NOT NULL,
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(20) NOT NULL,
   quote_id VARCHAR(20) NOT NULL,
   opening_price NUMERIC,
   highest_price NUMERIC,
   lowest_price NUMERIC,
   closing_price NUMERIC,
   volume NUMERIC
);
-- Create unique index on first columns
CREATE UNIQUE INDEX ohlcvs_exch_base_quote_time_idx ON ohlcvs (exchange, base_id, quote_id, "time" ASC);
-- Create index on the exchange column, time column, symbol column separately
CREATE INDEX ohlcvs_time_idx ON ohlcvs ("time" ASC);
CREATE INDEX ohlcvs_exch_time_idx ON ohlcvs (exchange, "time" ASC);
CREATE INDEX ohlcvs_base_quote_time_idx ON ohlcvs (base_id, quote_id, "time" ASC);
-- Insert with duplicate policy (psycopg2)
INSERT INTO ohlcvs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (exchange, base_id, quote_id, "time") DO NOTHING;
-- Execute prepared INSERT statement (psycopg2)
EXECUTE ohlcvs_rows_insert_stmt(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
-- Delete duplicate rows
delete from ohlcvs oh
where exists (
  select 1 from ohlcvs
  where exchange = oh.exchange and base_id = oh.base_id
   and quote_id = oh.quote_id  and "time" = oh."time"
);

-- Create OHLCV errors table for fetching errors
CREATE TABLE ohlcvs_errors (
   exchange VARCHAR(100) NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   start_date TIMESTAMPTZ NOT NULL,
   end_date TIMESTAMPTZ NOT NULL,
   time_frame VARCHAR(10) NOT NULL,
   ohlcv_section VARCHAR(30),
   resp_status_code SMALLINT,
   exception_class TEXT,
   exception_message TEXT
);
-- Create index on first 3 columns
CREATE UNIQUE INDEX ohlcvs_errors_idx ON ohlcvs_errors (exception_class, resp_status_code, exchange, symbol, start_date, end_date, time_frame, ohlcv_section);

-- Create symbol_exchange table with symbols and exchange information
CREATE TABLE symbol_exchange (
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(20) NOT NULL,
   quote_id VARCHAR(20) NOT NULL,
   symbol VARCHAR(40) NOT NULL
);
-- Create index on first 3 columns
CREATE UNIQUE INDEX exch_base_quote_idx ON symbol_exchange (exchange, base_id, quote_id);
CREATE INDEX symexch_exch_idx ON symbol_exchange (exchange);
CREATE INDEX symexch_base_idx ON symbol_exchange (base_id);
CREATE INDEX symexch_quote_idx ON symbol_exchange (quote_id);
-- Insert with duplicate policy (psycopg2)
INSERT INTO symbol_exchange VALUES (%s,%s,%s,%s)
ON CONFLICT (exchange, base_id, quote_id) DO NOTHING;


---- QUERY CASES ----
-- (0) Update and fill-null queries
-- (1) Chart queries but not aggregated
-- (2) Chart queries and aggregated
-- (3) Continous aggregates with different resolutions

-- (0) Update and fill-null queries
-- (0a) Get the latest timestamp for each symbol-exchange combination
-- So we can run update job for each of the combination
explain (analyze)
select ohlcvss.exchange, symexch.symbol, ohlcvss.time
from symbol_exchange symexch,
   lateral (
      select time, exchange, base_id, quote_id
      from ohlcvs
      where ohlcvs.exchange = symexch.exchange
         and base_id = symexch.base_id
         and quote_id = symexch.quote_id
      order by base_id, quote_id, time desc
      limit 1
   ) ohlcvss
order by time asc;

-- (0b) Get timestamp gaps so we can automatically fill missing data
-- Keep refresing/running this query until no rows are returned
with tn as (
    select ohlcvs."time" as time,
    lead(ohlcvs."time", 1) over (
            partition by ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id
            order by ohlcvs."time" asc
        ) as next_time,
    ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id, symexch.symbol
    from ohlcvs
      left join symbol_exchange as symexch
         on ohlcvs.exchange = symexch.exchange
            and ohlcvs.base_id = symexch.base_id
            and ohlcvs.quote_id = symexch.quote_id
    limit 100000
),
time_diff as (
    select *, EXTRACT(EPOCH FROM (next_time - "time")) AS difference
    from tn
)
select *
from time_diff
where difference > 60
order by "time" asc;

-- (0c) Get the latest timestamps for each exchange to serve
-- the same purpose as (4). However, this one is unecessarily complicated
with exchs as (
    select exchange from symbol_exchange group by exchange
),
latest_time_exch as (
    select oh.exchange, oh.time
    from exchs,
    lateral (
        select "time", exchange
        from ohlcvs
        where ohlcvs.exchange = exchs.exchange
        order by "time" desc
        limit 1
    ) oh
)
select ohlcvs."time" as time,
lead(ohlcvs."time", 1) over (
    partition by ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id
    order by ohlcvs."time" asc
    ) as next_time,
ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id
from ohlcvs
    left join latest_time_exch as lte
        on ohlcvs.exchange = lte.exchange
        and ohlcvs."time" < lte."time"
limit 100000;

-- (1) Chart queries but not aggregated
-- (1a) A typical query for chart showcase
explain (analyze)
select *
from ohlcvs
where exchange='bittrex'
   and base_id='BTC'
   and quote_id='USD'
order by time asc;

-- (1b) Variation of (1a) to return lists when using psycopg2
select row_to_json(t)
   from (
         select *
         from ohlcvs
         where exchange='bitfinex'
            and base_id='BTC'
            and quote_id='USD'
         order by time asc
         limit 5
   ) t;

-- (1c) Variation of (1a) to use time_bucket, where we query the last
-- closing price within a time bucket
explain (analyze)
select time_bucket('1 week', "time") as "interval",
   last(closing_price, "time") as last_closing_price
from ohlcvs
where exchange='bittrex'
   and base_id='BTC'
   and quote_id='USD'
group by "interval"
order by "interval" asc;

-- (1d) Another typical query for chart, we load minutely data for a symbol
explain (analyze)
SELECT
  "time" AS "time",
  exchange AS metric,
  closing_price
FROM ohlcvs
WHERE
  base_id = 'BTC' AND
  quote_id = 'USD'
ORDER BY exchange, time;

-- (1e) Another typical query for chart, with a filter on timestamp
explain (analyze)
SELECT
  "time" AS "time",
  exchange AS metric,
  opening_price
FROM ohlcvs
WHERE
  base_id = 'BTC' AND
  quote_id = 'USD' AND
  "time" > '2021-01-01'::timestamp
ORDER BY exchange, time;

-- (2) Chart queries and aggregated
-- May be used to create continuous aggregation
-- (2a) Daily OHLCV summary from minutely data
-- Can be used to create continuous aggregation for faster client chart loading time
explain (analyze)
select time_bucket('1 day', "time") as "bucket",
   exchange,
   base_id,
   quote_id,
   first(opening_price, "time") as "open",
   max(highest_price) as "high",
   min(lowest_price) as "low",
   last(closing_price, "time") as "close",
   sum(volume) as "volume"
from ohlcvs
group by exchange, base_id, quote_id, "bucket";

-- (2b) Moving average based on exchange, base and quote
-- Can be used to create continuous aggregation for faster client chart loading time
explain (analyze)
select ohlcvs."time" as "time",
   symexch.symbol,
   avg(ohlcvs.closing_price) over (order by ohlcvs."time" rows between 10079 preceding and current row)
      as moving_avg
   from ohlcvs
      left join symbol_exchange as symexch
      on ohlcvs.base_id = symexch.base_id
      and ohlcvs.quote_id = symexch.quote_id
      and ohlcvs.exchange = symexch.exchange
   where ohlcvs.exchange = 'bittrex'
      and ohlcvs.base_id = 'BTC'
      and ohlcvs.quote_id = 'USD'
   order by time ASC;

-- (3) Continous aggregates with different resolutions
-- (3a) Weekly OHLCV summary
-- Can be used to view chart for a period back 5 years
create materialized view ohlcvs_summary_weekly
with (timescaledb.continuous) as
   select time_bucket('1 week', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(opening_price, "time") as "open",
      max(highest_price) as "high",
      min(lowest_price) as "low",
      last(closing_price, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_weekly', NULL, '2021-02-10');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_weekly',
   start_offset => INTERVAL '3 weeks',
   end_offset   => INTERVAL '1 week',
   schedule_interval => INTERVAL '1 day');

-- (3b) Daily OHLCV summary
-- Can be used to view chart for a period back 1 year
create materialized view ohlcvs_summary_daily
with (timescaledb.continuous) as
   select time_bucket('1 day', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(opening_price, "time") as "open",
      max(highest_price) as "high",
      min(lowest_price) as "low",
      last(closing_price, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_daily', NULL, '2021-02-10');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_daily',
   start_offset => INTERVAL '3 days',
   end_offset   => INTERVAL '1 day',
   schedule_interval => INTERVAL '4 hours');