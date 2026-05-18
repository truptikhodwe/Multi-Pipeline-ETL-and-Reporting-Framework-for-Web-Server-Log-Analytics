package com.etl.db;

import java.sql.*;

/**
 * PostgresWriter – all PostgreSQL interactions for the ETL framework.
 * Results accumulate across runs (no clearResults) to enable cross-pipeline comparison.
 */
public class PostgresWriter implements AutoCloseable {

    private static final String URL  = "jdbc:postgresql://localhost:5432/etl_project";
    private static final String USER = "postgres";
    private static final String PASS = "postgres";

    private final Connection conn;

    public PostgresWriter() throws Exception {
        conn = DriverManager.getConnection(URL, USER, PASS);
        conn.setAutoCommit(true);
    }

    public Connection getConnection() { return conn; }

    // ── Run metadata ────────────────────────────────────────────────────────
    public int insertRun(String pipelineName, String queryName,
                         int batchId, int batchSize, double avgBatchSize,
                         int recordsProcessed, int malformedRecordCount,
                         long runtimeMs) throws Exception {

        String sql = "INSERT INTO runs " +
            "(pipeline_name,query_name,batch_id,batch_size,avg_batch_size," +
            "records_processed,malformed_record_count,runtime_ms) " +
            "VALUES (?,?,?,?,?,?,?,?) RETURNING run_id";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, pipelineName);
            ps.setString(2, queryName);
            ps.setInt(3, batchId);
            ps.setInt(4, batchSize);
            ps.setDouble(5, avgBatchSize);
            ps.setInt(6, recordsProcessed);
            ps.setInt(7, malformedRecordCount);
            ps.setLong(8, runtimeMs);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getInt("run_id");
        }
    }

    // ── Batch metadata ──────────────────────────────────────────────────────
    public void insertBatchMetadata(int runId, int batchId, String batchLabel,
                                    String sourceFile, int recordCount) throws Exception {
        String sql = "INSERT INTO batch_metadata " +
            "(run_id,batch_id,batch_label,source_file,record_count) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId); ps.setInt(2, batchId);
            ps.setString(3, batchLabel); ps.setString(4, sourceFile);
            ps.setInt(5, recordCount);
            ps.executeUpdate();
        }
    }

    // ── Malformed summary ───────────────────────────────────────────────────
    public void insertMalformedSummary(int runId, int batchId,
                                       int malformedCount, int totalCount) throws Exception {
        double pct = totalCount > 0 ? (100.0 * malformedCount / totalCount) : 0.0;
        String sql = "INSERT INTO malformed_records_summary " +
            "(run_id,batch_id,malformed_count,total_count,malformed_pct) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId); ps.setInt(2, batchId);
            ps.setInt(3, malformedCount); ps.setInt(4, totalCount);
            ps.setDouble(5, pct);
            ps.executeUpdate();
        }
    }

    // ── Query 1 ─────────────────────────────────────────────────────────────
    public void insertQuery1(int runId, String pipelineName, int batchId,
                             Timestamp executedAt, String logDate,
                             int statusCode, long requestCount, long totalBytes) throws Exception {
        String sql = "INSERT INTO query1_results " +
            "(run_id,pipeline_name,batch_id,executed_at,log_date,status_code,request_count,total_bytes)" +
            " VALUES (?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId); ps.setString(2, pipelineName); ps.setInt(3, batchId);
            ps.setTimestamp(4, executedAt); ps.setString(5, logDate);
            ps.setInt(6, statusCode); ps.setLong(7, requestCount); ps.setLong(8, totalBytes);
            ps.executeUpdate();
        }
    }

    // ── Query 2 ─────────────────────────────────────────────────────────────
    public void insertQuery2(int runId, String pipelineName, int batchId,
                             Timestamp executedAt, String resourcePath,
                             long requestCount, long totalBytes, long distinctHostCount) throws Exception {
        String sql = "INSERT INTO query2_results " +
            "(run_id,pipeline_name,batch_id,executed_at,resource_path,request_count,total_bytes,distinct_host_count)" +
            " VALUES (?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId); ps.setString(2, pipelineName); ps.setInt(3, batchId);
            ps.setTimestamp(4, executedAt); ps.setString(5, resourcePath);
            ps.setLong(6, requestCount); ps.setLong(7, totalBytes); ps.setLong(8, distinctHostCount);
            ps.executeUpdate();
        }
    }

    // ── Query 3 ─────────────────────────────────────────────────────────────
    public void insertQuery3(int runId, String pipelineName, int batchId,
                             Timestamp executedAt, String logDate, int logHour,
                             long errorRequestCount, long totalRequestCount,
                             double errorRate, long distinctErrorHosts) throws Exception {
        String sql = "INSERT INTO query3_results " +
            "(run_id,pipeline_name,batch_id,executed_at,log_date,log_hour," +
            "error_request_count,total_request_count,error_rate,distinct_error_hosts)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId); ps.setString(2, pipelineName); ps.setInt(3, batchId);
            ps.setTimestamp(4, executedAt); ps.setString(5, logDate); ps.setInt(6, logHour);
            ps.setLong(7, errorRequestCount); ps.setLong(8, totalRequestCount);
            ps.setDouble(9, errorRate); ps.setLong(10, distinctErrorHosts);
            ps.executeUpdate();
        }
    }

    @Override
    public void close() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }
}
