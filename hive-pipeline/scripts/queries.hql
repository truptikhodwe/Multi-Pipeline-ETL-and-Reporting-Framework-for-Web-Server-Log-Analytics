DROP TABLE IF EXISTS query1_results;
DROP TABLE IF EXISTS query2_results;
DROP TABLE IF EXISTS query3_results;

-- QUERY 1: Daily Traffic Summary
CREATE TABLE query1_results STORED AS ORC AS
SELECT
    log_date,
    status AS status_code,
    COUNT(*) AS request_count,
    SUM(bytes) AS total_bytes
FROM logs_parsed
GROUP BY log_date, status;

-- QUERY 2: Top 20 Requested Resources
CREATE TABLE query2_results STORED AS ORC AS
SELECT *
FROM (
    SELECT
        path AS resource_path,
        COUNT(*) AS request_count,
        SUM(bytes) AS total_bytes,
        COUNT(DISTINCT host) AS distinct_host_count
    FROM logs_parsed
    GROUP BY path
) t
ORDER BY request_count DESC
LIMIT 20;

-- QUERY 3: Hourly Error Analysis
CREATE TABLE query3_results STORED AS ORC AS
SELECT
    log_date,
    hour AS log_hour,
    SUM(CASE WHEN status >= 400 AND status <= 599 THEN 1 ELSE 0 END) AS error_request_count,
    COUNT(*) AS total_request_count,
    SUM(CASE WHEN status >= 400 AND status <= 599 THEN 1 ELSE 0 END) / COUNT(*) AS error_rate,
    COUNT(DISTINCT CASE WHEN status >= 400 AND status <= 599 THEN host END) AS distinct_error_hosts
FROM logs_parsed
GROUP BY log_date, hour