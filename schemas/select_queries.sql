---- QUERY CASES ----
-- (0) Update and fill-null queries
-- (1) Chart queries but not aggregated
----

-- (0) Update and fill-null queries
-- (0a) Get the latest timestamp for each symbol-exchange combination
-- So we can run update job for each of the combination
explain (analyze)
SELECT
   symexch.exchange,
   symexch.base_id,
   symexch.quote_id,
   temp.time
FROM symbol_exchange symexch,
   LATERAL (
      SELECT exchange, base_id, quote_id, time
      FROM ohlcvs
      WHERE ohlcvs.exchange = symexch.exchange
         AND base_id = symexch.base_id
         AND quote_id = symexch.quote_id -- Lateral reference
      ORDER BY base_id, quote_id, time DESC
      LIMIT 1
   ) AS temp;

-- (0b) Get timestamp gaps so we can automatically fill missing data
-- Keep refresing/running this query until no rows are returned
with tn as (
    select ohlcvs."time" as time,
    lead(ohlcvs."time", 1) over (
            partition by ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id
            order by ohlcvs."time" asc
        ) as next_time,
    ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id, symexch.symbol
    from ohlcvs
      left join symbol_exchange as symexch
         on ohlcvs.exchange = symexch.exchange
            and ohlcvs.base_id = symexch.base_id
            and ohlcvs.quote_id = symexch.quote_id
    limit 100000
),
time_diff as (
    select *, EXTRACT(EPOCH FROM (next_time - "time")) AS difference
    from tn
)
select *
from time_diff
where difference > 60
order by "time" asc; 

-- (0c) Get the latest timestamps for each exchange to serve
-- the same purpose as (). However, this one is unecessarily complicated
with exchs as (
    select exchange from symbol_exchange group by exchange
),
latest_time_exch as (
    select oh.exchange, oh.time
    from exchs,
    lateral (
        select "time", exchange
        from ohlcvs
        where ohlcvs.exchange = exchs.exchange
        order by "time" desc
        limit 1
    ) oh
)
select ohlcvs."time" as time,
lead(ohlcvs."time", 1) over (
    partition by ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id
    order by ohlcvs."time" asc
    ) as next_time,
ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id
from ohlcvs
    left join latest_time_exch as lte
        on ohlcvs.exchange = lte.exchange
        and ohlcvs."time" < lte."time"
limit 100000;

-- (0d) Get OHLC from ohlcvs table based on
--    exchange and symbol, using "time" column
--    as seconds since EPOCH
-- Can be used to load data for charts
with bq as (
   select exchange, base_id, quote_id
   from symbol_exchange
   where exchange='bitfinex' and symbol='ETHBTC'
)
select extract(epoch from oh.time) as "time",
   oh.opening_price as "open",
   oh.highest_price as "high",
   oh.lowest_price as "low",
   oh.closing_price as "close"
from ohlcvs oh
   inner join bq on oh.exchange=bq.exchange
      and oh.base_id=bq.base_id and oh.quote_id=bq.quote_id
limit 10000;

-- (0e) Another variant of the time gap check query
SELECT *
FROM (
   SELECT
      bucket,
      (LEAD(bucket, 1) OVER (
         PARTITION BY exchange, base_id, quote_id
         ORDER BY bucket ASC) - bucket) AS delta,
      exchange,
      base_id,
      quote_id
   FROM ohlcvs_summary_daily
) days
WHERE delta > INTERVAL '1 day';

-- (0f) Update symbol exchange inactive (nontrading) symbols
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

-- (1) Chart queries but not aggregated
-- (1a) A typical query for chart showcase
explain (analyze)
select *
from ohlcvs
where exchange='bittrex'
   and base_id='BTC'
   and quote_id='USD'
order by time asc;

-- (1b) Variation of (1a) to return lists when using psycopg2
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

-- (1c) Variation of (1a) to use time_bucket, where we query the last
-- closing price within a time bucket
explain (analyze)
select time_bucket('1 week', "time") as "interval",
   last(closing_price, "time") as last_closing_price
from ohlcvs
where exchange='bittrex'
   and base_id='BTC'
   and quote_id='USD'
group by "interval"
order by "interval" asc;

-- (1d) Another typical query for chart, we load minutely data for a symbol
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

-- (1e) Another typical query for chart, with a filter on timestamp
explain (analyze)
SELECT
  "time" AS "time",
  exchange AS metric,
  opening_price
FROM ohlcvs
WHERE
  base_id = 'BTC' AND
  quote_id = 'USD' AND
  "time" > '2021-07-09 08:50:00+00'::timestamp
ORDER BY exchange, time;

-- (1f) See how many distinct base-quote are in the ohlcvs table
--    after a certain timestamp
select count(distinct(base_id, quote_id))
from ohlcvs
where exchange = 'bitfinex'
   and time > '2021-08-10 19:38:00'::timestamp;
