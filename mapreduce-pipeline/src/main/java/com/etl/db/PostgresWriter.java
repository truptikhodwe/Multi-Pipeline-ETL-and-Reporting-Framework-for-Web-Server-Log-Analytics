package com.etl.db;

import com.etl.model.BatchInfo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class PostgresWriter implements AutoCloseable {

    private static final String URL = "jdbc:postgresql://localhost:5432/etl_project";
    private static final String USER = "postgres";
    private static final String PASS = "postgres";

    private final Connection conn;

    public PostgresWriter() throws Exception {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(URL, USER, PASS);
        conn.setAutoCommit(false);
    }

    public int insertRunMetrics(String pipeline, long runtimeMs, long totalRecords,
                                long malformedRecords, int totalBatches, double avgBatchSize) throws Exception {

        String sql = "INSERT INTO run_metrics " +
                "(pipeline, runtime_ms, total_records, malformed_records, total_batches, avg_batch_size) " +
                "VALUES (?, ?, ?, ?, ?, ?) RETURNING run_id";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, pipeline);
            ps.setLong(2, runtimeMs);
            ps.setLong(3, totalRecords);
            ps.setLong(4, malformedRecords);
            ps.setInt(5, totalBatches);
            ps.setDouble(6, avgBatchSize);

            ResultSet rs = ps.executeQuery();
            rs.next();
            int runId = rs.getInt("run_id");
            conn.commit();
            return runId;
        }
    }

    public void insertBatchMetadata(int runId, List<BatchInfo> batches) throws Exception {
        String sql = "INSERT INTO batch_metadata " +
                "(run_id, batch_id, batch_size, start_record, end_record) " +
                "VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (BatchInfo b : batches) {
                ps.setInt(1, runId);
                ps.setInt(2, b.batchId);
                ps.setInt(3, b.batchSize);
                ps.setLong(4, b.startRecord);
                ps.setLong(5, b.endRecord);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
    }

    public void loadQuery1Results(int runId, String outputPath) throws Exception {
        List<String> lines = readHdfsOutput(outputPath);

        String sql = "INSERT INTO query1_results " +
                "(run_id, log_date, status_code, request_count, total_bytes) " +
                "VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                ParsedLine parsed = parseKeyValueLine(line);
                if (parsed == null) continue;

                String[] keyParts = parsed.key.split("\\|");
                String[] valParts = parsed.value.split(",");

                if (keyParts.length < 2 || valParts.length < 2) continue;

                try {
                    ps.setInt(1, runId);
                    ps.setString(2, keyParts[0].trim());
                    ps.setInt(3, Integer.parseInt(keyParts[1].trim()));
                    ps.setLong(4, Long.parseLong(valParts[0].trim()));
                    ps.setLong(5, Long.parseLong(valParts[1].trim()));
                    ps.addBatch();
                } catch (Exception e) {
                    // skip bad row
                }
            }
            ps.executeBatch();
            conn.commit();
        }
    }

    public void loadQuery2Top20Results(int runId, String outputPath) throws Exception {
        List<Query2Row> rows = new ArrayList<>();
        List<String> lines = readHdfsOutput(outputPath);

        for (String line : lines) {
            ParsedLine parsed = parseKeyValueLine(line);
            if (parsed == null) continue;

            String[] valParts = parsed.value.split(",");
            if (valParts.length < 3) continue;

            try {
                Query2Row row = new Query2Row();
                row.path = parsed.key.trim();
                row.requestCount = Long.parseLong(valParts[0].trim());
                row.totalBytes = Long.parseLong(valParts[1].trim());
                row.distinctHosts = Long.parseLong(valParts[2].trim());
                rows.add(row);
            } catch (Exception e) {
                // skip bad row
            }
        }

        rows.sort(
                Comparator.comparingLong((Query2Row r) -> r.requestCount).reversed().thenComparing(r -> r.path)
        );

        String sql = "INSERT INTO query2_results " +
                "(run_id, resource_path, request_count, total_bytes, distinct_hosts) " +
                "VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int limit = Math.min(20, rows.size());
            for (int i = 0; i < limit; i++) {
                Query2Row r = rows.get(i);
                ps.setInt(1, runId);
                ps.setString(2, r.path);
                ps.setLong(3, r.requestCount);
                ps.setLong(4, r.totalBytes);
                ps.setLong(5, r.distinctHosts);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
    }

    public void loadQuery3Results(int runId, String outputPath) throws Exception {
        List<String> lines = readHdfsOutput(outputPath);

        String sql = "INSERT INTO query3_results " +
                "(run_id, log_date, log_hour, total_requests, error_requests, error_rate, distinct_error_hosts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                ParsedLine parsed = parseKeyValueLine(line);
                if (parsed == null) continue;

                String[] keyParts = parsed.key.split("\\|");
                String[] valParts = parsed.value.split(",");

                if (keyParts.length < 2 || valParts.length < 4) continue;

                try {
                    ps.setInt(1, runId);
                    ps.setString(2, keyParts[0].trim());
                    ps.setInt(3, Integer.parseInt(keyParts[1].trim()));
                    ps.setLong(4, Long.parseLong(valParts[0].trim()));
                    ps.setLong(5, Long.parseLong(valParts[1].trim()));
                    ps.setDouble(6, Double.parseDouble(valParts[2].trim()));
                    ps.setLong(7, Long.parseLong(valParts[3].trim()));
                    ps.addBatch();
                } catch (Exception e) {
                    // skip bad row
                }
            }
            ps.executeBatch();
            conn.commit();
        }
    }

    private List<String> readHdfsOutput(String outputPath) throws Exception {
        Process process = new ProcessBuilder(
                "hdfs", "dfs", "-cat", outputPath + "/part-r-00000"
        ).redirectErrorStream(true).start();

        BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getInputStream())
        );

        List<String> lines = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) {
                lines.add(line);
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0 && lines.isEmpty()) {
            throw new RuntimeException("Failed to read HDFS output: " + outputPath);
        }

        return lines;
    }

    private ParsedLine parseKeyValueLine(String line) {
        String[] split = line.split("\\t", 2);
        if (split.length < 2) return null;

        ParsedLine p = new ParsedLine();
        p.key = split[0].trim();
        p.value = split[1].trim();
        return p;
    }

    private static class ParsedLine {
        String key;
        String value;
    }

    private static class Query2Row {
        String path;
        long requestCount;
        long totalBytes;
        long distinctHosts;
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}