-- For use with psycopg2
-- Before using the queries...
-- psql -h localhost -p 5432 -U postgres -W -d postgres
-- password is in .env file

-- Create OHLCVS table
-- Create Symbol exchange table
-- Create OHLCVS errors table
-- Create test table (resembles OHLCVS)
CREATE TABLE ohlcvs (
   time TIMESTAMPTZ NOT NULL,
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(20) NOT NULL,
   quote_id VARCHAR(20) NOT NULL,
   open NUMERIC,
   high NUMERIC,
   low NUMERIC,
   close NUMERIC,
   volume NUMERIC
);

CREATE TABLE symbol_exchange (
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(20) NOT NULL,
   quote_id VARCHAR(20) NOT NULL,
   symbol VARCHAR(40) NOT NULL
);

CREATE TABLE ohlcvs_errors (
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

CREATE TABLE test (
   id NUMERIC NOT NULL,
   b VARCHAR(20) NOT NULL,
   q VARCHAR(20) NOT NULL,
   o NUMERIC,
   c NUMERIC
)


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
CREATE INDEX ohlcvs_time_idx ON ohlcvs ("time" ASC);
CREATE INDEX ohlcvs_exch_time_idx ON ohlcvs (exchange, "time" ASC);
CREATE INDEX ohlcvs_base_quote_time_idx ON ohlcvs (base_id, quote_id, "time" ASC);

CREATE UNIQUE INDEX symexch_exch_sym_idx ON symbol_exchange (exchange, symbol);
CREATE INDEX symexch_exch_idx ON symbol_exchange (exchange);
CREATE INDEX symexch_base_idx ON symbol_exchange (base_id);
CREATE INDEX symexch_quote_idx ON symbol_exchange (quote_id);


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
