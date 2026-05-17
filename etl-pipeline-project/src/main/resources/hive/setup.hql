-- Hive Setup – External table with RegexSerDe for Apache logs
-- Run once before executing any query scripts

CREATE DATABASE IF NOT EXISTS etl_logs;
USE etl_logs;

DROP TABLE IF EXISTS raw_logs;

CREATE EXTERNAL TABLE raw_logs (
    host          STRING,
    log_date      STRING,
    log_hour      STRING,
    method        STRING,
    resource_path STRING,
    protocol      STRING,
    status_code   INT,
    bytes_text    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)"
)
STORED AS TEXTFILE
LOCATION '/input/logs';

-- Cleaned view: batch_id derived from virtual_filename (Jul→1, Aug→2)
DROP TABLE IF EXISTS valid_logs;

CREATE TABLE valid_logs STORED AS ORC AS
SELECT
    CASE WHEN INPUT__FILE__NAME LIKE '%Aug%' THEN 2 ELSE 1 END AS batch_id,
    host,
    log_date,
    CAST(log_hour AS INT)                                        AS log_hour,
    method,
    resource_path,
    protocol,
    status_code,
    CASE WHEN bytes_text RLIKE '^[0-9]+$'
         THEN CAST(bytes_text AS BIGINT) ELSE 0 END             AS bytes
FROM raw_logs
WHERE host IS NOT NULL
  AND log_date IS NOT NULL
  AND log_hour IS NOT NULL
  AND resource_path IS NOT NULL
  AND status_code IS NOT NULL;
