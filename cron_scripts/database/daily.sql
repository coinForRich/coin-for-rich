-- (0) Update inactive ebq (symbols)
WITH
   ebq_timediff AS (
      SELECT
         symexch.exchange,
         symexch.base_id,
         symexch.quote_id,
         (NOW() - temp.time) as diff
      FROM symbol_exchange symexch,
         LATERAL (
            SELECT exchange, base_id, quote_id, time
            FROM ohlcvs
            WHERE ohlcvs.exchange = symexch.exchange
               AND base_id = symexch.base_id
               AND quote_id = symexch.quote_id -- Lateral reference
            ORDER BY base_id, quote_id, time DESC
            LIMIT 1
         ) AS temp
   ),
   ebq_active AS (
      SELECT
         e.exchange,
         e.base_id,
         e.quote_id,
         (
            CASE
               WHEN diff <= INTERVAL '1 day' THEN true
               ELSE false
            END
         ) AS is_trading
      FROM ebq_timediff AS e
      ORDER BY exchange, base_id, quote_id
   )
   
UPDATE symbol_exchange AS se
SET is_trading = ebq_active.is_trading
FROM ebq_active
WHERE se.exchange = ebq_active.exchange
   AND se.base_id = ebq_active.base_id
   AND se.quote_id = ebq_active.quote_id;

-- (1) Materialized views for analytics
REFRESH MATERIALIZED VIEW CONCURRENTLY geo_daily_return;
REFRESH MATERIALIZED VIEW CONCURRENTLY top_20_quoted_vol;
REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_return;
