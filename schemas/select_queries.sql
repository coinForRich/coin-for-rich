---- QUERY CASES ----
-- (0) Update and fill-null queries
-- (1) Chart queries but not aggregated
----

-- (0) Update and fill-null queries
-- (0a) Get the latest timestamp for each symbol-exchange combination
-- So we can run update job for each of the combination
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
   ) ohlcvss
order by time asc;

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
-- the same purpose as (4). However, this one is unecessarily complicated
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
