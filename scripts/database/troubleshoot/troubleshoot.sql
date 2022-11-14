-- View continuous aggregates' materialized views and their respective
-- refresh/retention schedules
SELECT
    cont_aggs.view_name AS view_name,
    jobs.hypertable_name,
    jobs.job_id,
    jobs.schedule_interval,
    job_stats.job_status,
    job_stats.last_run_status,
    job_stats.next_start,
    job_stats.total_successes,
    job_stats.total_failures
FROM timescaledb_information.jobs AS jobs
LEFT JOIN timescaledb_information.job_stats job_stats
    ON jobs.job_id = job_stats.job_id
LEFT JOIN timescaledb_information.continuous_aggregates AS cont_aggs
    ON jobs.hypertable_schema = cont_aggs.materialization_hypertable_schema
    AND jobs.hypertable_name = cont_aggs.materialization_hypertable_name
;


-- View a continuous aggregates' materialized view and its data retention job status
-- This example is for ohlcvs_summary_5min
SELECT
    cont_aggs.view_name AS view_name,
    job_stats.*
FROM timescaledb_information.jobs AS jobs
LEFT JOIN timescaledb_information.job_stats job_stats
    ON jobs.job_id = job_stats.job_id
LEFT JOIN timescaledb_information.continuous_aggregates AS cont_aggs
    ON jobs.hypertable_schema = cont_aggs.materialization_hypertable_schema
    AND jobs.hypertable_name = cont_aggs.materialization_hypertable_name
WHERE cont_aggs.view_name = 'ohlcvs_summary_5min'
;
