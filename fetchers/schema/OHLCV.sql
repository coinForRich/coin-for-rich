-- For use with psycopg2
-- Before using the queries...
-- psql -h localhost -p 15432 -U postgres -W -d postgres
-- password is horus123

-- Create OHLCVS table
CREATE TABLE ohlcvs (
   time TIMESTAMPTZ NOT NULL,
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(10) NOT NULL,
   quote_id VARCHAR(10) NOT NULL,
   opening_price NUMERIC,
   highest_price NUMERIC,
   lowest_price NUMERIC,
   closing_price NUMERIC,
   volume NUMERIC
);
-- Create index on first columns
CREATE UNIQUE INDEX ohlcvs_exch_base_quote_time_idx ON ohlcvs (exchange, base_id, quote_id, "time" ASC);
-- Create index on the exchange column, time column, symbol column separately
CREATE INDEX ohlcvs_time_idx ON ohlcvs ("time" ASC);
CREATE INDEX ohlcvs_exch_time_idx ON ohlcvs (exchange, "time" ASC);
CREATE INDEX ohlcvs_base_quote_time_idx ON ohlcvs (base_id, quote_id, "time" ASC);
-- CREATE INDEX ohlcvs_baseid_time_idx ON ohlcvs (base_id, time asc);
-- CREATE INDEX ohlcvs_quoteid_time_idx ON ohlcvs (quote_id, time asc);

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

-- Create symbol_exchange table with symbols and exchange information
CREATE TABLE symbol_exchange (
   exchange VARCHAR(100) NOT NULL,
   base_id VARCHAR(10) NOT NULL,
   quote_id VARCHAR(10) NOT NULL,
   symbol VARCHAR(20) NOT NULL
);
-- Create index on first 3 columns
CREATE UNIQUE INDEX exch_base_quote_idx ON symbol_exchange (exchange, base_id, quote_id);
CREATE INDEX symexch_exch_idx ON  symbol_exchange (exchange);
CREATE INDEX symexch_base_idx ON symbol_exchange (base_id);
CREATE INDEX symexch_quote_idx ON symbol_exchange (quote_id);

-- (0) Get the latest timestamp for each symbol-exchange combination
-- So we can run update for them
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
   ) ohlcvss;

-- (1) A typical query for chart showcase
explain (analyze)
select *
from ohlcvs
where exchange='bittrex'
   and base_id='BTC'
   and quote_id='USD'
order by time asc;

-- (1a) Variation of (1) to return lists when using psycopg2
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

-- (1b) A variation of (1) to use time_bucket
explain (analyze)
select time_bucket('1 week', "time") as "interval",
   last(closing_price, "time") as last_closing_price
from ohlcvs
where exchange='bittrex'
   and base_id='BTC'
   and quote_id='USD'
group by "interval"
order by "interval" asc;

-- (2) Similar typical query for chart
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

-- (2a) Similar typical query for chart, with filter on timestamp
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

-- (3) Moving average based on exchange, base and quote
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