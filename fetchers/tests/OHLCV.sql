-- For use with psycopg2
-- Before using the queries...
-- psql -h localhost -p 15432 -U postgres -W -d postgres
-- password is horus123

-- Create OHLCVS table
CREATE TABLE ohlcvs (
   time TIMESTAMPTZ NOT NULL,
   exchange VARCHAR(100) NOT NULL,
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
CREATE UNIQUE INDEX ohlcvs_ts_exch_basequote ON ohlcvs (time, exchange, base_id, quote_id);
-- Create index on the exchange column, time column, symbol column separately
CREATE INDEX ohlcvs_time_idx ON ohlcvs (time);
CREATE INDEX ohlcvs_exchange_idx ON ohlcvs (exchange);
CREATE INDEX ohlcvs_time_basequote_idx ON ohlcvs(time, base_id, quote_id);

CREATE INDEX ohlcvs_baseidquoteid_idx ON ohlcvs (base_id, quote_id); --Time for typical query (1): 13376.208 ms
-- Compare `ohlcvs_baseidquoteid_idx` with these ones -- time for typical queyr (1): about 14000 ms - almost the same as above. Advantage of individual indices: user may only query on one column:
CREATE INDEX ohlcvs_baseid_idx ON ohlcvs (base_id);
CREATE INDEX ohlcvs_quoteid_idx ON ohlcvs (quote_id);

-- Insert with duplicate policy (psycopg2)
INSERT INTO ohlcvs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (time, exchange, symbol) DO NOTHING;
-- Execute prepared INSERT statement (psycopg2)
EXECUTE ohlcvs_rows_insert_stmt(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);

-- Create OHLCV errors table
CREATE TABLE ohlcvs_errors (
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

-- Create table with symbols and exchange information
CREATE TABLE symbol_exchange (
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(10) NOT NULL,
   quote_id VARCHAR(10) NOT NULL,
   symbol VARCHAR(20) NOT NULL
);
-- Create index on first 3 columns
CREATE UNIQUE INDEX exch_base_quote_idx ON symbol_exchange (exchange, base_id, quote_id);

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


-- Get the latest timestamp for each symbol-exchange combination
-- So we can run update for them
explain (analyze, format json)
select se.symbol, ohlcvss.time, ohlcvss.exchange
from symbol_exchange se,
   lateral (
      select time, exchange, base_id, quote_id
      from ohlcvs
      where base_id = se.base_id
         and quote_id = se.quote_id
      order by time desc
      limit 1
   ) ohlcvss;

-- (1) A typical query for chart showcase
select *
from ohlcvs
where exchange='bitfinex'
   and base_id='BTC'
   and quote_id='USD'
order by time asc;