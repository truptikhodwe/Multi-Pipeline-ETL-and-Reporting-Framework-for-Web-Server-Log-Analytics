%default INPUT '/input/logs'
%default OUTPUT '/output'
%default BATCH_SIZE '1000'

--------------------------------------------------
-- LOAD
--------------------------------------------------

raw_logs = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

--------------------------------------------------
-- ASSIGNING SEQUENTIAL RECORD NUMBER AND BATCH ID
--------------------------------------------------

ranked_logs = RANK raw_logs;

logs = FOREACH ranked_logs GENERATE
    (int)(FLOOR((rank_raw_logs - 1) / (double)$BATCH_SIZE) + 1) AS batch_id,
    line AS line;

--------------------------------------------------
-- PARSE USING REGEX
--------------------------------------------------

parsed = FOREACH logs GENERATE
    batch_id AS batch_id,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 1) AS host,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 2) AS date,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 3) AS hour,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 4) AS method,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 5) AS path,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 6) AS protocol,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 7) AS status_text,
    REGEX_EXTRACT(line, '^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)', 8) AS bytes_text;

normalized = FOREACH parsed GENERATE
    batch_id,
    host,
    date AS log_date,
    hour AS log_hour,
    method,
    path AS resource_path,
    protocol,
    (int)status_text AS status_code,
    ((bytes_text MATCHES '\\d+') ? (long)bytes_text : 0L) AS bytes;

valid_logs = FILTER normalized BY
    host IS NOT NULL AND
    log_date IS NOT NULL AND
    log_hour IS NOT NULL AND
    resource_path IS NOT NULL AND
    status_code IS NOT NULL AND
    bytes IS NOT NULL;

STORE valid_logs INTO '$OUTPUT/valid_logs' USING PigStorage('\t');

--------------------------------------------------
-- QUERY 1: DAILY TRAFFIC SUMMARY
-- Output:
-- log_date, status_code, request_count, total_bytes
--------------------------------------------------

q1_grouped = GROUP valid_logs BY (batch_id, log_date, status_code);

q1_results = FOREACH q1_grouped GENERATE
    group.batch_id AS batch_id,
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(valid_logs) AS request_count,
    SUM(valid_logs.bytes) AS total_bytes;

q1_ordered = ORDER q1_results BY log_date ASC, status_code ASC;

STORE q1_ordered INTO '$OUTPUT/query1' USING PigStorage('\t');

--------------------------------------------------
-- QUERY 2: TOP 20 REQUESTED RESOURCES
-- Output:
-- resource_path, request_count, total_bytes, distinct_host_count
--------------------------------------------------

-- Request count and total bytes per resource path
q2_grouped = GROUP valid_logs BY (batch_id, resource_path);

q2_results = FOREACH q2_grouped {
    unique_hosts = DISTINCT valid_logs.host;

    GENERATE
        group.batch_id AS batch_id,
        group.resource_path AS resource_path,
        COUNT(valid_logs) AS request_count,
        SUM(valid_logs.bytes) AS total_bytes,
        COUNT(unique_hosts) AS distinct_host_count;
};

q2_ordered = ORDER q2_results BY request_count DESC, resource_path ASC;

q2_top20 = LIMIT q2_ordered 20;

STORE q2_top20 INTO '$OUTPUT/query2' USING PigStorage('\t');

--------------------------------------------------
-- QUERY 3: HOURLY ERROR ANALYSIS
-- Output:
-- batch_id, log_date, log_hour, error_request_count,
-- total_request_count, error_rate, distinct_error_hosts
--------------------------------------------------

-- Total requests per batch/date/hour
q3_total_grouped = GROUP valid_logs BY (batch_id, log_date, log_hour);

q3_total_counts = FOREACH q3_total_grouped GENERATE
    group.batch_id AS batch_id,
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT(valid_logs) AS total_request_count;

-- Error requests only
error_logs = FILTER valid_logs BY status_code >= 400 AND status_code <= 599;

q3_error_grouped = GROUP error_logs BY (batch_id, log_date, log_hour);

q3_error_counts = FOREACH q3_error_grouped GENERATE
    group.batch_id AS batch_id,
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT(error_logs) AS error_request_count;

-- Distinct error hosts per batch/date/hour
q3_error_host_rows = FOREACH error_logs GENERATE
    batch_id AS batch_id,
    log_date AS log_date,
    log_hour AS log_hour,
    host AS host;

q3_distinct_error_host_rows = DISTINCT q3_error_host_rows;

q3_error_host_grouped = GROUP q3_distinct_error_host_rows BY (batch_id, log_date, log_hour);

q3_distinct_error_hosts = FOREACH q3_error_host_grouped GENERATE
    group.batch_id AS batch_id,
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT(q3_distinct_error_host_rows) AS distinct_error_hosts;

q3_join_errors = JOIN
    q3_total_counts BY (batch_id, log_date, log_hour) LEFT OUTER,
    q3_error_counts BY (batch_id, log_date, log_hour);

q3_join_hosts = JOIN
    q3_join_errors BY (q3_total_counts::batch_id, q3_total_counts::log_date, q3_total_counts::log_hour) LEFT OUTER,
    q3_distinct_error_hosts BY (batch_id, log_date, log_hour);

q3_results = FOREACH q3_join_hosts GENERATE
    q3_total_counts::batch_id AS batch_id,
    q3_total_counts::log_date AS log_date,
    q3_total_counts::log_hour AS log_hour,
    (
        q3_error_counts::error_request_count IS NULL
        ? 0L
        : q3_error_counts::error_request_count
    ) AS error_request_count,
    q3_total_counts::total_request_count AS total_request_count,
    (
        (double)(
            q3_error_counts::error_request_count IS NULL
            ? 0L
            : q3_error_counts::error_request_count
        ) / (double)q3_total_counts::total_request_count
    ) AS error_rate,
    (
        q3_distinct_error_hosts::distinct_error_hosts IS NULL
        ? 0L
        : q3_distinct_error_hosts::distinct_error_hosts
    ) AS distinct_error_hosts;

q3_ordered = ORDER q3_results BY log_date ASC, log_hour ASC;

STORE q3_ordered INTO '$OUTPUT/query3' USING PigStorage('\t');