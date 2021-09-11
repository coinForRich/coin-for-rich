-- Continuous aggregates
CALL refresh_continuous_aggregate('ohlcvs_summary_daily', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcvs_summary_5min', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcvs_summary_15min', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcvs_summary_30min', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcvs_summary_1hour', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcvs_summary_6hour', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcvs_summary_12hour', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcvs_summary_7day', NULL, NULL);

-- Materialized views for analytics
REFRESH MATERIALIZED VIEW CONCURRENTLY geo_daily_return;
REFRESH MATERIALIZED VIEW CONCURRENTLY top_20_quoted_vol;
REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_return;
