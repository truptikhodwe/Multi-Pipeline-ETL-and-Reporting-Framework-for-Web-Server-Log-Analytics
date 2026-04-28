INSERT INTO TABLE logs_parsed
SELECT
    regexp_extract(line, '^(\\S+)', 1),
    regexp_extract(regexp_extract(line, '\\[([^\\]]+)\\]', 1), '^(\\d{2}/\\w{3}/\\d{4})', 1),
    CAST(regexp_extract(regexp_extract(line, '\\[([^\\]]+)\\]', 1), ':(\\d{2}):', 1) AS INT),
    split(regexp_extract(line, '"(.*?)"', 1), ' ')[0],
    split(regexp_extract(line, '"(.*?)"', 1), ' ')[1],
    split(regexp_extract(line, '"(.*?)"', 1), ' ')[2],
    CAST(regexp_extract(line, '" (\\d{3}) ', 1) AS INT),
    CASE
        WHEN regexp_extract(line, '\\s(\\S+)$', 1) = '-' THEN 0
        ELSE CAST(regexp_extract(line, '\\s(\\S+)$', 1) AS BIGINT)
    END
FROM logs_raw
WHERE line RLIKE '^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] "(.*?)" (\\d{3}) (\\S+)$'