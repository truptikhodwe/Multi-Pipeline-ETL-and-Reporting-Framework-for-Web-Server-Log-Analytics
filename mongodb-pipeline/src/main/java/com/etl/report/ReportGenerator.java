package com.etl.report;

import java.sql.*;

public class ReportGenerator {

    private Connection conn;

    public ReportGenerator() throws Exception {
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/etl_project",
                "postgres",
                "postgres"
        );
    }

    public void generateReport(int runId) throws Exception {

        System.out.println("\n========== REPORT ==========");

        // Run info
        PreparedStatement ps1 = conn.prepareStatement(
                "SELECT * FROM runs WHERE run_id = ?"
        );
        ps1.setInt(1, runId);
        ResultSet rs1 = ps1.executeQuery();

        while (rs1.next()) {
            System.out.println("Pipeline: " + rs1.getString("pipeline_name"));
            System.out.println("Runtime: " + rs1.getLong("runtime_ms"));
            System.out.println("Batch Size: " + rs1.getInt("batch_size"));
            System.out.println("Avg Batch: " + rs1.getDouble("avg_batch_size"));
            System.out.println("Total Records: " + rs1.getInt("total_records"));
            System.out.println("Malformed: " + rs1.getInt("malformed_records"));
        }

        // Query 1 sample
        System.out.println("\n--- Query 1 Sample ---");
        Statement st = conn.createStatement();
        ResultSet rs2 = st.executeQuery("SELECT * FROM query1_results WHERE run_id = " + runId + " LIMIT 5");

        while (rs2.next()) {
            System.out.println(
                    rs2.getString("log_date") + " | " +
                    rs2.getInt("status_code") + " | " +
                    rs2.getInt("request_count")
            );
        }
    }
}