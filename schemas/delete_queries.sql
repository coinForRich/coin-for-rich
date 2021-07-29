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
