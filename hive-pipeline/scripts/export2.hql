INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive_export/query2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT resource_path, request_count, total_bytes, distinct_host_count
FROM query2_results