package com.etl.db;

import java.sql.*;

public class PostgresWriter implements AutoCloseable {

    private Connection conn;

    public PostgresWriter() throws Exception {

        String url = "jdbc:postgresql://localhost:5432/etl_project";
        String user = "postgres";
        String password = "postgres"; // change if needed

        conn = DriverManager.getConnection(url, user, password);
    }

    public void clearResults() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DELETE FROM query1_results");
            stmt.executeUpdate("DELETE FROM query2_results");
            stmt.executeUpdate("DELETE FROM query3_results");
        }
    }

    // ---------------- RUN METADATA ----------------
    public int insertRun(String pipeline,
                         long runtime,
                         int batchSize,
                         double avgBatchSize,
                         int totalRecords,
                         int malformedRecords) throws Exception {

        String sql = "INSERT INTO runs " +
                "(pipeline_name, runtime_ms, batch_size, avg_batch_size, total_records, malformed_records) " +
                "VALUES (?, ?, ?, ?, ?, ?) RETURNING run_id";

        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setString(1, pipeline);
        ps.setLong(2, runtime);
        ps.setInt(3, batchSize);
        ps.setDouble(4, avgBatchSize);
        ps.setInt(5, totalRecords);
        ps.setInt(6, malformedRecords);

        ResultSet rs = ps.executeQuery();
        rs.next();

        return rs.getInt("run_id");
    }

    // ---------------- QUERY 1 ----------------
    public void insertQuery1(int runId,
                             String pipelineName,
                             int batchId,
                             Timestamp executedAt,
                             String date,
                             int status,
                             long requestCount,
                             long totalBytes) throws Exception {
        String sql = """
            INSERT INTO query1_results (
                run_id,
                pipeline_name,
                batch_id,
                executed_at,
                log_date,
                status_code,
                request_count,
                total_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            ps.setString(2, pipelineName);
            ps.setInt(3, batchId);
            ps.setTimestamp(4, executedAt);
            ps.setString(5, date);
            ps.setInt(6, status);
            ps.setLong(7, requestCount);
            ps.setLong(8, totalBytes);
            ps.executeUpdate();
        }
    }

    // ---------------- QUERY 2 ----------------
    public void insertQuery2(int runId,
                             String pipelineName,
                             int batchId,
                             Timestamp executedAt,
                             String path,
                             long requestCount,
                             long totalBytes,
                             long distinctHostCount) throws Exception {
        String sql = """
            INSERT INTO query2_results (
                run_id,
                pipeline_name,
                batch_id,
                executed_at,
                resource_path,
                request_count,
                total_bytes,
                distinct_host_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            ps.setString(2, pipelineName);
            ps.setInt(3, batchId);
            ps.setTimestamp(4, executedAt);
            ps.setString(5, path);
            ps.setLong(6, requestCount);
            ps.setLong(7, totalBytes);
            ps.setLong(8, distinctHostCount);
            ps.executeUpdate();
        }
    }

    // ---------------- QUERY 3 ----------------
    public void insertQuery3(int runId,
                             String pipelineName,
                             int batchId,
                             Timestamp executedAt,
                             String date,
                             int hour,
                             long errorRequestCount,
                             long totalRequestCount,
                             double errorRate,
                             long distinctErrorHosts) throws Exception {
        String sql = """
            INSERT INTO query3_results (
                run_id,
                pipeline_name,
                batch_id,
                executed_at,
                log_date,
                log_hour,
                error_request_count,
                total_request_count,
                error_rate,
                distinct_error_hosts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            ps.setString(2, pipelineName);
            ps.setInt(3, batchId);
            ps.setTimestamp(4, executedAt);
            ps.setString(5, date);
            ps.setInt(6, hour);
            ps.setLong(7, errorRequestCount);
            ps.setLong(8, totalRequestCount);
            ps.setDouble(9, errorRate);
            ps.setLong(10, distinctErrorHosts);
            ps.executeUpdate();
        }
    }

    // ---------------- CLOSE CONNECTION ----------------
    @Override
    public void close() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}