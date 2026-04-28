package com.etl.db;

import java.sql.*;

public class PostgresWriter {

    private Connection conn;

    public PostgresWriter() throws Exception {
        String url = "jdbc:postgresql://localhost:5432/etl_project";
        String user = "postgres";
        String password = "postgres"; // change if needed

        conn = DriverManager.getConnection(url, user, password);
    }

    public int insertRun(String pipeline, long runtime, int batchSize,
                         double avgBatchSize, int total, int malformed) throws Exception {

        String sql = "INSERT INTO runs (pipeline_name, runtime_ms, batch_size, avg_batch_size, total_records, malformed_records) VALUES (?, ?, ?, ?, ?, ?) RETURNING run_id";

        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, pipeline);
        ps.setLong(2, runtime);
        ps.setInt(3, batchSize);
        ps.setDouble(4, avgBatchSize);
        ps.setInt(5, total);
        ps.setInt(6, malformed);

        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getInt("run_id");
    }

    public void insertQuery1(int runId, String date, int status, int count, long bytes) throws Exception {
        String sql = "INSERT INTO query1_results VALUES (?, ?, ?, ?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setInt(1, runId);
        ps.setString(2, date);
        ps.setInt(3, status);
        ps.setInt(4, count);
        ps.setLong(5, bytes);

        ps.executeUpdate();
    }

    public void insertQuery2(int runId, String path, int count, long bytes, int hosts) throws Exception {
        String sql = "INSERT INTO query2_results VALUES (?, ?, ?, ?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setInt(1, runId);
        ps.setString(2, path);
        ps.setInt(3, count);
        ps.setLong(4, bytes);
        ps.setInt(5, hosts);

        ps.executeUpdate();
    }

    public void insertQuery3(int runId, String date, int hour,
        int errorCount, int totalRequests,
        double errorRate, int hosts) throws Exception {

        String sql = "INSERT INTO query3_results VALUES (?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setInt(1, runId);
        ps.setString(2, date);
        ps.setInt(3, hour);
        ps.setInt(4, errorCount);
        ps.setInt(5, totalRequests);
        ps.setDouble(6, errorRate);
        ps.setInt(7, hosts);

        ps.executeUpdate();
    }

    public void insertBatch(int runId, int batchId, int batchSize) throws Exception {
        String sql = "INSERT INTO batch_metadata (run_id, batch_id, batch_size) VALUES (?, ?, ?)";
    
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, runId);
        ps.setInt(2, batchId);
        ps.setInt(3, batchSize);
    
        ps.executeUpdate();
    }
}