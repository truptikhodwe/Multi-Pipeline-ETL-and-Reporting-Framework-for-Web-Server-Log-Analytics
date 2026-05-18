USE etl_logs;

-- Query 1: Daily Traffic Summary
-- Output: batch_id, log_date, status_code, request_count, total_bytes
INSERT OVERWRITE DIRECTORY '/output/query1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    log_date,
    status_code,
    COUNT(*)        AS request_count,
    SUM(bytes)      AS total_bytes
FROM valid_logs
GROUP BY batch_id, log_date, status_code
ORDER BY log_date ASC, status_code ASC;
