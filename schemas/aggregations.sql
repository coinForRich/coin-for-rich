---- QUERY CASES ----
-- (2) Chart queries and aggregated
-- (3) Continous aggregates with different resolutions
-- (4) More aggregates for dashboard
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

CALL refresh_continuous_aggregate('ohlcvs_summary_daily', NULL, '2021-08-01');

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

CALL refresh_continuous_aggregate('ohlcvs_summary_5min', NULL, '2021-08-01');

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

CALL refresh_continuous_aggregate('ohlcvs_summary_15min', NULL, '2021-08-01');

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

CALL refresh_continuous_aggregate('ohlcvs_summary_30min', NULL, '2021-08-01');

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

CALL refresh_continuous_aggregate('ohlcvs_summary_1hour', NULL, '2021-08-01');

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

CALL refresh_continuous_aggregate('ohlcvs_summary_6hour', NULL, '2021-08-01');

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

CALL refresh_continuous_aggregate('ohlcvs_summary_12hour', NULL, '2021-08-01');

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

CALL refresh_continuous_aggregate('ohlcvs_summary_7day', NULL, '2021-08-01');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_7day',
   start_offset => INTERVAL '21 days',
   end_offset   => INTERVAL '7 days',
   schedule_interval => INTERVAL '7 days');

-- (4a) Geometric average daily return since the last 7 days
-- Used to create a dashboard ranking daily profitability of a symbol
-- Dates with no ohlcv data will be filled with the earliest date before
--    that has ohlcv data
EXPLAIN (ANALYZE)
CREATE MATERIALIZED VIEW top_500_daily_return AS
   WITH 
      close_filled AS (
         SELECT
            generate_series(
               bucket,
               LEAD(bucket, 1) OVER (
                  PARTITION BY exchange, base_id, quote_id ORDER BY bucket
                  ) - interval '1 day',
               interval '1 day'
            )::date AS bucket,
            exchange,
            base_id,
            quote_id,
            close
         FROM ohlcvs_summary_daily
         WHERE bucket >= (CURRENT_DATE - interval '8 days')
            AND close <> 0
            -- AND exchange='bittrex' AND base_id='GBYTE' AND quote_id='BTC'
      ),
      prev_close_view AS (
         SELECT 
            *,
            LAG(close) OVER (
               PARTITION BY exchange, base_id, quote_id
               ORDER BY bucket ASC) AS prev_close
         FROM close_filled
      ),
      daily_factor AS (
         SELECT
            bucket,
            exchange,
            base_id,
            quote_id,
            LN(close/prev_close) AS ln_daily_factor
         FROM prev_close_view
      ),

      -- SELECT *
      -- FROM daily_factor;

      avg_daily_return AS (
         SELECT
            exchange,
            base_id,
            quote_id,
            CAST(
               POWER(EXP(SUM(ln_daily_factor)), (1.0/COUNT(*))) - 1 AS NUMERIC(10, 4)
            ) AS gavg_daily_return
         FROM daily_factor
         WHERE ln_daily_factor IS NOT NULL
         GROUP BY exchange, base_id, quote_id
      )
   SELECT
      ROW_NUMBER() OVER (ORDER BY gavg_daily_return DESC) AS ranking,
      *
   FROM avg_daily_return
   ORDER BY gavg_daily_return DESC
   LIMIT 500;

CREATE UNIQUE INDEX top_500_dr_idx ON top_500_daily_return (exchange, base_id, quote_id);

-- (4b) The top XXXX commodities (bases)
--    with the highest trading volume in the past week
-- Can be used for a pie chart
CREATE MATERIALIZED VIEW top_10_vol_bases AS
   WITH base_ttl_vol AS ( --Base total vol
         SELECT base_id, SUM(volume) AS total_volume
         FROM ohlcvs_summary_7day
         WHERE bucket >= (CURRENT_DATE - interval '8 days')
         GROUP BY base_id
      ),
      bgrp_vol AS ( --Base group total vol
         SELECT
            (CASE
               WHEN ranking > 10 THEN 'Other'
               ELSE base_id
            END) AS bgrp,
            total_volume
         FROM (
            SELECT
               *,
               ROW_NUMBER() OVER(ORDER BY total_volume DESC) AS ranking
            FROM base_ttl_vol
            ) AS temp
         )
      SELECT
         bgrp AS base_id,
         ROUND(SUM(total_volume), 4) AS ttl_vol
      FROM bgrp_vol
      GROUP BY bgrp;

CREATE UNIQUE INDEX top_10_vlmb_idx ON top_10_vol_bases (base_id);
