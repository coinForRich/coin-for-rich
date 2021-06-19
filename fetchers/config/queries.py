# This module contains queries related to fetching

# Generic insert query that ignores unique constraints
PSQL_INSERT_IGNOREDUP_QUERY = "INSERT INTO {table} VALUES %s ON CONFLICT DO NOTHING;"

LATEST_SYMEXCH_QUERY = '''
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
'''

TS_GAPS_QUERY = '''
select row_to_json(results)
from (
   with tn as (
      select ohlcvs."time" as time,
      lead(ohlcvs."time", 1) over (
               partition by ohlcvs.exchange, ohlcvs.base_id, ohlcvs.quote_id
               order by ohlcvs."time" asc
         ) as next_time,
      ohlcvs.exchange, symexch.symbol
      from ohlcvs
         left join symbol_exchange as symexch
            on ohlcvs.exchange = symexch.exchange
               and ohlcvs.base_id = symexch.base_id
               and ohlcvs.quote_id = symexch.quote_id
      limit 10000
   ),
   time_diff as (
      select *, EXTRACT(EPOCH FROM (next_time - "time")) AS difference
      from tn
   )
   select *
   from time_diff
   where difference > 60
   order by "time" asc
) results;
'''