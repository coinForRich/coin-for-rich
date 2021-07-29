---- QUERY CASES ----
-- (2) Chart queries and aggregated
-- (3) Continous aggregates with different resolutions
----

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

CALL refresh_continuous_aggregate('ohlcvs_summary_daily', NULL, '2021-06-26');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_daily',
   start_offset => INTERVAL '3 days',
   end_offset   => INTERVAL '1 day',
   schedule_interval => INTERVAL '1 day');

-- (3c) 5-min OHLCV summary
-- Can be used to view chart for a period back 6 hours
create materialized view ohlcvs_summary_5min
with (timescaledb.continuous) as
   select time_bucket('5 minutes', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_5min', NULL, '2021-06-26');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_5min',
   start_offset => INTERVAL '15 minutes',
   end_offset   => INTERVAL '5 minutes',
   schedule_interval => INTERVAL '5 minutes');

-- (3d) 15-min OHLCV summary
-- Can be used to view chart for a period back 1 day
create materialized view ohlcvs_summary_15min
with (timescaledb.continuous) as
   select time_bucket('15 minutes', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_15min', NULL, '2021-06-26');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_15min',
   start_offset => INTERVAL '45 minutes',
   end_offset   => INTERVAL '15 minutes',
   schedule_interval => INTERVAL '15 minutes');

-- (3e) 30-min OHLCV summary
-- Can be used to view chart for a period back 3 days
create materialized view ohlcvs_summary_30min
with (timescaledb.continuous) as
   select time_bucket('30 minutes', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_30min', NULL, '2021-06-26');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_30min',
   start_offset => INTERVAL '90 minutes',
   end_offset   => INTERVAL '30 minutes',
   schedule_interval => INTERVAL '30 minutes');

-- (3f) 1-hour OHLCV summary
-- Can be used to view chart for a period back 7 days
create materialized view ohlcvs_summary_1hour
with (timescaledb.continuous) as
   select time_bucket('1 hour', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_1hour', NULL, '2021-06-26');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_1hour',
   start_offset => INTERVAL '3 hours',
   end_offset   => INTERVAL '1 hour',
   schedule_interval => INTERVAL '1 hour');

-- (3g) 6-hour OHLCV summary
-- Can be used to view chart for a period back 1 month
create materialized view ohlcvs_summary_6hour
with (timescaledb.continuous) as
   select time_bucket('6 hours', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_6hour', NULL, '2021-06-26');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_6hour',
   start_offset => INTERVAL '18 hours',
   end_offset   => INTERVAL '6 hours',
   schedule_interval => INTERVAL '6 hours');

-- (3h) 12-hour OHLCV summary
-- Can be used to view chart for a period back 3 months
create materialized view ohlcvs_summary_12hour
with (timescaledb.continuous) as
   select time_bucket('12 hours', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_12hour', NULL, '2021-06-26');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_12hour',
   start_offset => INTERVAL '36 hours',
   end_offset   => INTERVAL '12 hours',
   schedule_interval => INTERVAL '12 hours');

-- (3i) 7-day OHLCV summary
-- Can be used to view chart for a period back 3 years
create materialized view ohlcvs_summary_7day
with (timescaledb.continuous) as
   select time_bucket('7 days', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   from ohlcvs
   group by exchange, base_id, quote_id, "bucket"
with no data;

CALL refresh_continuous_aggregate('ohlcvs_summary_7day', NULL, '2021-07-10');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_7day',
   start_offset => INTERVAL '21 days',
   end_offset   => INTERVAL '7 days',
   schedule_interval => INTERVAL '7 days');
