-- Automatically creates tables and views


-- Timescaledb extension
CREATE EXTENSION IF NOT EXISTS timescaledb;


-- Create OHLCVS table
-- Create Symbol exchange table
-- Create OHLCVS errors table
-- Create test table (resembles OHLCVS)
CREATE TABLE IF NOT EXISTS ohlcvs (
   time TIMESTAMPTZ NOT NULL,
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(20) NOT NULL,
   quote_id VARCHAR(20) NOT NULL,
   open NUMERIC NOT NULL,
   high NUMERIC NOT NULL,
   low NUMERIC NOT NULL,
   close NUMERIC NOT NULL,
   volume NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS symbol_exchange (
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(20) NOT NULL,
   quote_id VARCHAR(20) NOT NULL,
   symbol VARCHAR(40) NOT NULL,
   is_trading BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS ohlcvs_errors (
   exchange VARCHAR(100) NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   start_date TIMESTAMPTZ NOT NULL,
   end_date TIMESTAMPTZ NOT NULL,
   time_frame VARCHAR(10) NOT NULL,
   ohlcv_section VARCHAR(30),
   resp_status_code SMALLINT,
   exception_class TEXT NOT NULL,
   exception_message TEXT
);

CREATE TABLE IF NOT EXISTS test (
   id NUMERIC NOT NULL,
   b VARCHAR(20) NOT NULL,
   q VARCHAR(20) NOT NULL,
   o NUMERIC,
   c NUMERIC
);


-- Create primary key constraints
ALTER TABLE ohlcvs
ADD PRIMARY KEY (exchange, base_id, quote_id, "time");

ALTER TABLE symbol_exchange
ADD PRIMARY KEY (exchange, base_id, quote_id);

ALTER TABLE ohlcvs_errors
ADD PRIMARY KEY (exception_class, exchange, symbol, start_date, end_date, time_frame);

ALTER TABLE test
ADD PRIMARY KEY (id, b, q);


-- Create foreign key constraints
ALTER TABLE ohlcvs 
ADD CONSTRAINT exch_base_quote_fkey 
FOREIGN KEY (exchange, base_id, quote_id)
REFERENCES symbol_exchange (exchange, base_id, quote_id)
ON DELETE CASCADE;


-- Create indices
CREATE INDEX IF NOT EXISTS ohlcvs_time_idx ON ohlcvs ("time" ASC);
CREATE INDEX IF NOT EXISTS ohlcvs_exch_time_idx ON ohlcvs (exchange, "time" ASC);
CREATE INDEX IF NOT EXISTS ohlcvs_base_quote_time_idx ON ohlcvs (base_id, quote_id, "time" ASC);

CREATE UNIQUE INDEX IF NOT EXISTS symexch_exch_sym_idx ON symbol_exchange (exchange, symbol);
CREATE INDEX IF NOT EXISTS symexch_exch_idx ON symbol_exchange (exchange);
CREATE INDEX IF NOT EXISTS symexch_base_idx ON symbol_exchange (base_id);
CREATE INDEX IF NOT EXISTS symexch_quote_idx ON symbol_exchange (quote_id);


-- Create timescaledb hypertable
SELECT create_hypertable('ohlcvs', 'time');


-- Create materialized view for common base - quote among exchanges
-- The condition on COUNT() can change as more exchanges are added
-- This view is temporarily used to choose which symbols to fetch
--    ohlcvs data, because storage is limited
CREATE MATERIALIZED VIEW common_basequote_30 AS
   SELECT base_id, quote_id
   FROM symbol_exchange
   GROUP BY base_id, quote_id HAVING COUNT(*) > 2
   ORDER BY base_id ASC, quote_id ASC
   LIMIT 30;


-- Continous aggregations

-- Create
CREATE MATERIALIZED VIEW ohlcvs_summary_daily
WITH (timescaledb.continuous) AS
   SELECT time_bucket('1 day', "time") AS "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") AS "open",
      max(high) AS "high",
      min(low) AS "low",
      last(close, "time") AS "close",
      sum(volume) AS "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE MATERIALIZED VIEW ohlcvs_summary_5min
WITH (timescaledb.continuous) AS
   SELECT time_bucket('5 minutes', "time") as "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE materialized view ohlcvs_summary_15min
WITH (timescaledb.continuous) AS
   SELECT time_bucket('15 minutes', "time") AS "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE materialized view ohlcvs_summary_30min
WITH (timescaledb.continuous) AS
   SELECT time_bucket('30 minutes', "time") AS "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE materialized view ohlcvs_summary_1hour
WITH (timescaledb.continuous) AS
   SELECT time_bucket('1 hour', "time") AS "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE materialized view ohlcvs_summary_6hour
WITH (timescaledb.continuous) AS
   SELECT time_bucket('6 hours', "time") AS "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE materialized view ohlcvs_summary_12hour
WITH (timescaledb.continuous) AS
   SELECT time_bucket('12 hours', "time") AS "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE materialized view ohlcvs_summary_7day
WITH (timescaledb.continuous) AS
   SELECT time_bucket('7 days', "time") AS "bucket",
      exchange,
      base_id,
      quote_id,
      first(open, "time") as "open",
      max(high) as "high",
      min(low) as "low",
      last(close, "time") as "close",
      sum(volume) as "volume"
   FROM ohlcvs
   GROUP BY exchange, base_id, quote_id, "bucket"
WITH NO DATA;

CREATE MATERIALIZED VIEW geo_daily_return AS
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
      )

      -- SELECT *
      -- FROM daily_factor;

   SELECT
      exchange,
      base_id,
      quote_id,
      CAST(
         (POWER(EXP(SUM(ln_daily_factor)), (1.0/COUNT(*))) - 1) * 100
            AS NUMERIC(10, 4)
      ) AS daily_return_pct
   FROM daily_factor
   WHERE ln_daily_factor IS NOT NULL
   GROUP BY exchange, base_id, quote_id
   ORDER BY daily_return_pct DESC
WITH NO DATA;

CREATE MATERIALIZED VIEW top_20_quoted_vol AS
   WITH
      ebq_quoted_vol AS (
         SELECT
            exchange, base_id, quote_id,
            close * volume AS quoted_vol
         FROM ohlcvs_summary_7day
         WHERE bucket >= (CURRENT_DATE - interval '8 days')
      ),
      bq_quoted_vol AS (
         SELECT
            base_id, quote_id, SUM(quoted_vol) AS ttl_quoted_vol
         FROM ebq_quoted_vol
         GROUP BY base_id, quote_id
      ),
      bqgrp_qoute_vol AS (
         SELECT
            (CASE
               WHEN ranking > 20 THEN 'Other'
               ELSE concat(base_id, '-', quote_id)
            END) AS bqgrp,
            ttl_quoted_vol
         FROM (
            SELECT
               *,
               ROW_NUMBER() OVER(ORDER BY ttl_quoted_vol DESC) AS ranking
            FROM bq_quoted_vol
         ) AS temp
         ORDER BY ttl_quoted_vol DESC
      )

   SELECT
      bqgrp,
      ROUND(SUM(ttl_quoted_vol), 4) AS total_volume
   FROM bqgrp_qoute_vol
   GROUP BY bqgrp
WITH NO DATA;

CREATE MATERIALIZED VIEW weekly_return AS
   SELECT
      bucket AS time,
      exchange, base_id, quote_id,
      ROUND(((close_price - open_price) / open_price) * 100, 4) AS weekly_return_pct
   FROM (
      SELECT DISTINCT ON (exchange, base_id, quote_id)
         time_bucket('1 week', time) as bucket,
         exchange, base_id, quote_id,
         first(open, time) as open_price,
         last(close, time) as close_price
      FROM ohlcvs
      WHERE time >= (CURRENT_DATE - interval '1 week')
      GROUP BY exchange, base_id, quote_id, bucket
      ORDER BY exchange, base_id, quote_id, bucket DESC
   ) temp
   WHERE close_price IS NOT NULL
      AND open_price IS NOT NULL and open_price <> 0
   ORDER BY weekly_return_pct DESC
WITH NO DATA;  

-- Indices on materialized views
CREATE UNIQUE INDEX geo_dr_idx ON geo_daily_return (exchange, base_id, quote_id);
CREATE UNIQUE INDEX top_20_qvlm_idx ON top_20_quoted_vol (bqgrp);
CREATE UNIQUE INDEX wr_idx ON weekly_return (exchange, base_id, quote_id, time);

-- Schedule continous aggregations
SELECT add_continuous_aggregate_policy('ohlcvs_summary_daily',
   start_offset => INTERVAL '3 days',
   end_offset   => INTERVAL '1 day',
   schedule_interval => INTERVAL '1 day');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_5min',
   start_offset => INTERVAL '15 minutes',
   end_offset   => INTERVAL '5 minutes',
   schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_15min',
   start_offset => INTERVAL '45 minutes',
   end_offset   => INTERVAL '15 minutes',
   schedule_interval => INTERVAL '15 minutes');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_30min',
   start_offset => INTERVAL '90 minutes',
   end_offset   => INTERVAL '30 minutes',
   schedule_interval => INTERVAL '30 minutes');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_1hour',
   start_offset => INTERVAL '3 hours',
   end_offset   => INTERVAL '1 hour',
   schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_6hour',
   start_offset => INTERVAL '18 hours',
   end_offset   => INTERVAL '6 hours',
   schedule_interval => INTERVAL '6 hours');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_12hour',
   start_offset => INTERVAL '36 hours',
   end_offset   => INTERVAL '12 hours',
   schedule_interval => INTERVAL '12 hours');

SELECT add_continuous_aggregate_policy('ohlcvs_summary_7day',
   start_offset => INTERVAL '21 days',
   end_offset   => INTERVAL '7 days',
   schedule_interval => INTERVAL '7 days');
