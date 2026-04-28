INSERT INTO TABLE logs_malformed
SELECT line FROM logs_raw
WHERE NOT (line RLIKE '^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] "(.*?)" (\\d{3}) (\\S+)$')