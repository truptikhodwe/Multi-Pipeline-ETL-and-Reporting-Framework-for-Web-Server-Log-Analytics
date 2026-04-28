INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive_export/query1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT log_date, status_code, request_count, total_bytes
FROM query1_results