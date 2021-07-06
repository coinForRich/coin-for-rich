-- For use with psycopg2
-- Before using the queries...
-- psql -h localhost -p 5432 -U postgres -W -d postgres
-- password is in ENV file

-- Create OHLCVS table and hyptertable
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
-- Create timescaledb hypertable
SELECT create_hypertable('ohlcvs', 'time');
-- Create primary key constraint
ALTER TABLE ohlcvs
ADD PRIMARY KEY (exchange, base_id, quote_id, "time");
-- Create index on the exchange column, time column, symbol column separately
CREATE INDEX ohlcvs_time_idx ON ohlcvs ("time" ASC);
CREATE INDEX ohlcvs_exch_time_idx ON ohlcvs (exchange, "time" ASC);
CREATE INDEX ohlcvs_base_quote_time_idx ON ohlcvs (base_id, quote_id, "time" ASC);
-- Create foreign key constraint
ALTER TABLE ohlcvs 
ADD CONSTRAINT exch_base_quote_fkey 
FOREIGN KEY (exchange, base_id, quote_id)
REFERENCES symbol_exchange (exchange, base_id, quote_id)
ON DELETE CASCADE;
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
   and ohlcvs.ctid > oh.ctid
);
-- Rename columns
ALTER TABLE ohlcvs
RENAME closing_price TO "close";

-- Create OHLCV errors table for fetching errors
CREATE TABLE ohlcvs_errors (
   exchange VARCHAR(100) NOT NULL,
   symbol VARCHAR(20) NOT NULL,
   start_date TIMESTAMPTZ NOT NULL,
   end_date TIMESTAMPTZ NOT NULL,
   time_frame VARCHAR(10) NOT NULL,
   exception_class TEXT NOT NULL,
   ohlcv_section VARCHAR(30),
   resp_status_code SMALLINT,
   exception_message TEXT
);
-- Create primary key constraint
ALTER TABLE ohlcvs_errors
ADD PRIMARY KEY (exception_class, exchange, symbol, start_date, end_date, time_frame);

-- Create symbol_exchange table with symbols and exchange information
CREATE TABLE symbol_exchange (
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(20) NOT NULL,
   quote_id VARCHAR(20) NOT NULL,
   symbol VARCHAR(40) NOT NULL
);
-- Create primary key constraint
ALTER TABLE symbol_exchange
ADD PRIMARY KEY (exchange, base_id, quote_id);
-- Create indices
CREATE INDEX symexch_exch_idx ON symbol_exchange (exchange);
CREATE INDEX symexch_base_idx ON symbol_exchange (base_id);
CREATE INDEX symexch_quote_idx ON symbol_exchange (quote_id);
-- Insert with duplicate policy (psycopg2)
INSERT INTO symbol_exchange VALUES (%s,%s,%s,%s)
ON CONFLICT (exchange, base_id, quote_id) DO NOTHING;

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
-- Delete from ohlcvs rows where base and quote not found
--    in the common_basequote_30 view
DELETE
FROM ohlcvs
WHERE NOT EXISTS
   (SELECT *
   FROM common_basequote_30 cb
   WHERE cb.base_id = ohlcvs.base_id
      AND cb.quote_id = ohlcvs.quote_id
   );