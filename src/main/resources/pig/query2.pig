%default INPUT '/input/logs'
%default OUTPUT '/output'
%default BATCH_SIZE '1000'

-- ============================================================
-- SHARED: Load, Rank, Parse, Normalize, Filter
-- ============================================================

raw_logs = LOAD '$INPUT' USING PigStorage('\u0001', '-tagFile') AS (filename:chararray, line:chararray);

logs = FOREACH raw_logs GENERATE
    (filename MATCHES '.*Aug.*' ? 2 : 1) AS batch_id,
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
-- QUERY 2: TOP 20 REQUESTED RESOURCES
-- Output cols: batch_id, resource_path, request_count, total_bytes, distinct_host_count
-- ============================================================

q2_grouped = GROUP valid_logs BY (batch_id, resource_path);

q2_results = FOREACH q2_grouped {
    unique_hosts = DISTINCT valid_logs.host;
    GENERATE
        group.batch_id      AS batch_id,
        group.resource_path AS resource_path,
        COUNT(valid_logs)   AS request_count,
        SUM(valid_logs.bytes) AS total_bytes,
        COUNT(unique_hosts) AS distinct_host_count;
};

q2_ordered = ORDER q2_results BY request_count DESC, resource_path ASC;
q2_top20   = LIMIT q2_ordered 20;

STORE q2_top20 INTO '$OUTPUT/query2' USING PigStorage('\t');
