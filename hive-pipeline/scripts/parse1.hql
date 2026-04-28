DROP TABLE IF EXISTS logs_parsed;
DROP TABLE IF EXISTS logs_malformed;

CREATE TABLE logs_parsed (
    host STRING,
    log_date STRING,
    hour INT,
    method STRING,
    path STRING,
    protocol STRING,
    status INT,
    bytes BIGINT
)
STORED AS ORC;

CREATE TABLE logs_malformed (
    line STRING
)
STORED AS TEXTFILE