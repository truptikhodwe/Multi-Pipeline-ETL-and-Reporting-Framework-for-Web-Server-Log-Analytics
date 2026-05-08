%default INPUT '/input/logs'
%default OUTPUT '/output'
%default BATCH_SIZE '1000'

-- ============================================================
-- SHARED: Load, Rank, Parse, Normalize, Filter
-- ============================================================

raw_logs = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

ranked_logs = RANK raw_logs;

logs = FOREACH ranked_logs GENERATE
    (int)(FLOOR((rank_raw_logs - 1) / (double)$BATCH_SIZE) + 1) AS batch_id,
    line AS line;

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

-- ============================================================
-- QUERY 1: DAILY TRAFFIC SUMMARY
-- Output cols: batch_id, log_date, status_code, request_count, total_bytes
-- ============================================================

q1_grouped = GROUP valid_logs BY (batch_id, log_date, status_code);

q1_results = FOREACH q1_grouped GENERATE
    group.batch_id    AS batch_id,
    group.log_date    AS log_date,
    group.status_code AS status_code,
    COUNT(valid_logs) AS request_count,
    SUM(valid_logs.bytes) AS total_bytes;

q1_ordered = ORDER q1_results BY log_date ASC, status_code ASC;

STORE q1_ordered INTO '$OUTPUT/query1' USING PigStorage('\t');
