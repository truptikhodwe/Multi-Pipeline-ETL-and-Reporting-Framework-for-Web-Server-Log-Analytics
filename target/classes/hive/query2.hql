USE etl_logs;

-- Query 2: Top 20 Requested Resources
-- Output: batch_id, resource_path, request_count, total_bytes, distinct_host_count
INSERT OVERWRITE DIRECTORY '/output/query2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    resource_path,
    COUNT(*)                AS request_count,
    SUM(bytes)              AS total_bytes,
    COUNT(DISTINCT host)    AS distinct_host_count
FROM valid_logs
GROUP BY batch_id, resource_path
ORDER BY request_count DESC
LIMIT 20;
