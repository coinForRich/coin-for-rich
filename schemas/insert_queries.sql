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

-- Insert with duplicate policy (psycopg2)
INSERT INTO symbol_exchange VALUES (%s,%s,%s,%s)
ON CONFLICT (exchange, base_id, quote_id) DO NOTHING;
