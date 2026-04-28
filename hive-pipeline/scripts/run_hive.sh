#!/bin/bash

RUN_ID="hive_$(date +%Y%m%d%H%M%S)"
PIPELINE="hive"
BATCH_ID=1
PG_DB="nasalogs"
PG_USER="postgres"

echo "=== Starting Hive ETL Pipeline ==="
echo "Run ID: $RUN_ID"

START_TIME=$(date +%s)

# Run all Hive scripts
echo "--- Running schema setup ---"
hive -f schema.hql

echo "--- Creating parsed/malformed tables ---"
hive -f parse1.hql

echo "--- Parsing valid records ---"
hive -f parse2.hql

echo "--- Parsing malformed records ---"
hive -f parse3.hql

echo "--- Running queries ---"
hive -f queries.hql

echo "--- Exporting results ---"
hive -f export1.hql
hive -f export2.hql
hive -f export3.hql

END_TIME=$(date +%s)
RUNTIME=$((END_TIME - START_TIME))

# Merge part files
cat /tmp/hive_export/query1/000* > /tmp/hive_export/q1_final.csv
cat /tmp/hive_export/query2/000* > /tmp/hive_export/q2_final.csv
cat /tmp/hive_export/query3/000* > /tmp/hive_export/q3_final.csv

# Get batch size = total records processed
BATCH_SIZE=$(hive -e "SELECT COUNT(*) FROM logs_raw" 2>/dev/null | tail -1)

echo "--- Loading into PostgreSQL ---"
psql -U $PG_USER -d $PG_DB << EOF

-- Insert run metadata
INSERT INTO run_metadata VALUES (
    '$RUN_ID', '$PIPELINE', $BATCH_ID, $BATCH_SIZE, $RUNTIME, NOW()
);

-- Load query 1
\COPY query1_results(log_date, status_code, request_count, total_bytes)
FROM '/tmp/hive_export/q1_final.csv' DELIMITER ',' CSV;

UPDATE query1_results SET run_id = '$RUN_ID' WHERE run_id IS NULL;

-- Load query 2
\COPY query2_results(resource_path, request_count, total_bytes, distinct_host_count)
FROM '/tmp/hive_export/q2_final.csv' DELIMITER ',' CSV;

UPDATE query2_results SET run_id = '$RUN_ID' WHERE run_id IS NULL;

-- Load query 3
\COPY query3_results(log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)
FROM '/tmp/hive_export/q3_final.csv' DELIMITER ',' CSV;

UPDATE query3_results SET run_id = '$RUN_ID' WHERE run_id IS NULL;

EOF

echo "=== Done ==="
echo "Run ID:     $RUN_ID"
echo "Pipeline:   $PIPELINE"
echo "Batch ID:   $BATCH_ID"
echo "Batch Size: $BATCH_SIZE"
echo "Runtime:    ${RUNTIME}s"