USE etl_logs;

-- Query 3: Hourly Error Analysis
-- Output: batch_id, log_date, log_hour, error_count, total_count, error_rate, distinct_error_hosts

INSERT OVERWRITE DIRECTORY '/output/query3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    t.batch_id,
    t.log_date,
    t.log_hour,
    COALESCE(e.error_count, 0)                                          AS error_request_count,
    t.total_count                                                        AS total_request_count,
    COALESCE(e.error_count, 0) / CAST(t.total_count AS DOUBLE)         AS error_rate,
    COALESCE(e.distinct_error_hosts, 0)                                 AS distinct_error_hosts
FROM (
    SELECT batch_id, log_date, log_hour, COUNT(*) AS total_count
    FROM valid_logs
    GROUP BY batch_id, log_date, log_hour
) t
LEFT JOIN (
    SELECT batch_id, log_date, log_hour,
           COUNT(*) AS error_count,
           COUNT(DISTINCT host) AS distinct_error_hosts
    FROM valid_logs
    WHERE status_code BETWEEN 400 AND 599
    GROUP BY batch_id, log_date, log_hour
) e
ON t.batch_id = e.batch_id AND t.log_date = e.log_date AND t.log_hour = e.log_hour
ORDER BY t.log_date ASC, t.log_hour ASC;
