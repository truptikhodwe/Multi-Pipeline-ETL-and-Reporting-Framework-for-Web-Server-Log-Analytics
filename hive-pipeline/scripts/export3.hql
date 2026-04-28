INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive_export/query3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
FROM query3_results