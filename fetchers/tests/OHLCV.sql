-- For use with psycopg2
-- Before using the queries...
-- psql -h localhost -p 15432 -U postgres -W -d postgres
-- password is horus123

DROP TABLE IF EXISTS "bittrex_ohlcv";
DROP TABLE IF EXISTS "bitfinex_ohlcv";

-- Create OHLCV table
CREATE TABLE ohlcvs (
   time TIMESTAMPTZ NOT NULL,
   exchange VARCHAR(100) NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   base_id VARCHAR(10),
   quote_id VARCHAR(10),
   opening_price DOUBLE PRECISION,
   highest_price DOUBLE PRECISION,
   lowest_price DOUBLE PRECISION,
   closing_price DOUBLE PRECISION,
   volume DOUBLE PRECISION,
   fetch_starting_time TIMESTAMPTZ NOT NULL
);
-- Create index on first 3 columns
CREATE UNIQUE INDEX ts_exch_sym ON ohlcvs (time, exchange, symbol);
-- Create index on the exchange column, time column, symbol column separately
CREATE INDEX ohlcvs_time_idx ON ohlcvs (time);
CREATE INDEX ohlcvs_exchange_idx ON ohlcvs (exchange);
CREATE INDEX ohlcvs_symbol_idx ON ohlcvs (symbol);
-- Insert with duplicate policy (psycopg2)
INSERT INTO ohlcvs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (time, exchange, symbol) DO NOTHING;
-- Execute prepared INSERT statement (psycopg2)
EXECUTE ohlcvs_rows_insert_stmt(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);

-- Create OHLCV errors table
CREATE TABLE ohlcvs_errors(
   fetch_start_time TIMESTAMPTZ NOT NULL,
   exchange VARCHAR(100) NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   start_date TIMESTAMPTZ NOT NULL,
   time_frame VARCHAR(10) NOT NULL,
   ohlcv_section VARCHAR(30),
   resp_status_code SMALLINT,
   exception_class TEXT,
   exception_message TEXT
);
-- Create index on first 3 columns
CREATE INDEX fetch_sym_st ON ohlcvs_errors (fetch_start_time, exchange, symbol, start_date);

-- Test table
CREATE TABLE test0 (
   time INTEGER NOT NULL,
   exchange VARCHAR(30) NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   val INTEGER NOT NULL
);
-- Create index on first 3 columns
CREATE UNIQUE INDEX ts_exch_sym_test0 ON test0 (time, exchange, symbol);
-- Insert with duplicate policy
INSERT INTO ts_exch_sym_test0 VALUES (%s,%s,%s,%s)
ON CONFLICT (time, exchange, symbol) DO NOTHING;

CREATE TABLE bittrex_ohlcv (
   time TIMESTAMPTZ NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   base_id VARCHAR(10),
   quote_id VARCHAR(10),
   opening_price DOUBLE PRECISION,
   highest_price DOUBLE PRECISION,
   lowest_price DOUBLE PRECISION,
   closing_price DOUBLE PRECISION,
   volume DOUBLE PRECISION,
   fetch_starting_time TIMESTAMPTZ NOT NULL
);
INSERT INTO bittrex_ohlcv VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);

CREATE TABLE bittrex_ohlcv_errors(
   fetch_start_time TIMESTAMPTZ NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   start_date TIMESTAMPTZ NOT NULL,
   interval VARCHAR(10) NOT NULL,
   ohlcv_section VARCHAR(30) NOT NULL,
   resp_status_code SMALLINT,
   exception_message TEXT NOT NULL
);

CREATE TABLE bitfinex_ohlcv (
   time TIMESTAMPTZ NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   base_id VARCHAR(10),
   quote_id VARCHAR(10),
   opening_price DOUBLE PRECISION,
   highest_price DOUBLE PRECISION,
   lowest_price DOUBLE PRECISION,
   closing_price DOUBLE PRECISION,
   volume DOUBLE PRECISION,
   fetch_starting_time TIMESTAMPTZ NOT NULL
);
INSERT INTO bitfinex_ohlcv VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);

CREATE TABLE bitfinex_ohlcv_errors(
   fetch_start_time TIMESTAMPTZ NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   start_date TIMESTAMPTZ NOT NULL,
   time_frame VARCHAR(10) NOT NULL,
   ohlcv_section VARCHAR(30) NOT NULL,
   resp_status_code SMALLINT,
   exception_message TEXT NOT NULL
);