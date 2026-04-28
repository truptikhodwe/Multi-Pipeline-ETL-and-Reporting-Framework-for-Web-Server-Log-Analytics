DROP TABLE IF EXISTS logs_raw;

CREATE EXTERNAL TABLE logs_raw (
    line STRING
)
STORED AS TEXTFILE
LOCATION '/nasa_logs';

SELECT COUNT(*) AS total_lines FROM logs_raw;