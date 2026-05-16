package com.etl.report;

import java.sql.*;

/**
 * ReportGenerator - queries PostgreSQL and prints a formatted run summary.
 */
public class ReportGenerator {

    public static void printReport(int runId, Connection conn) throws Exception {

        System.out.println();
        System.out.println("======================================================");
        System.out.println("                  ETL RUN REPORT");
        System.out.println("======================================================");

        // Run metadata
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pipeline_name, query_name, batch_id, batch_size, " +
                "avg_batch_size, records_processed, malformed_record_count, " +
                "runtime_ms, executed_at FROM runs WHERE run_id = ?")) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    System.out.printf("%n  Run ID            : %d%n", runId);
                    System.out.printf("  Pipeline          : %s%n", rs.getString("pipeline_name"));
                    System.out.printf("  Query             : %s%n", rs.getString("query_name"));
                    System.out.printf("  Batch ID          : %d%n", rs.getInt("batch_id"));
                    System.out.printf("  Batch Size        : %d%n", rs.getInt("batch_size"));
                    System.out.printf("  Avg Batch Size    : %.2f%n", rs.getDouble("avg_batch_size"));
                    System.out.printf("  Records Processed : %d%n", rs.getInt("records_processed"));
                    System.out.printf("  Malformed Records : %d%n", rs.getInt("malformed_record_count"));
                    System.out.printf("  Runtime           : %d ms%n", rs.getLong("runtime_ms"));
                    System.out.printf("  Executed At       : %s%n", rs.getTimestamp("executed_at"));
                }
            }
        }

        // Malformed summary
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT malformed_count, total_count, malformed_pct " +
                "FROM malformed_records_summary WHERE run_id = ?")) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    System.out.println();
                    System.out.println("  -- Malformed Records Summary --");
                    System.out.printf("     Malformed : %d / %d (%.2f%%)%n",
                        rs.getInt("malformed_count"),
                        rs.getInt("total_count"),
                        rs.getDouble("malformed_pct"));
                }
            }
        }

        // Query 1 results
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT log_date, status_code, request_count, total_bytes " +
                "FROM query1_results WHERE run_id = ? ORDER BY log_date, status_code LIMIT 10")) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                boolean hasRows = false;
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    if (!hasRows) {
                        sb.append("\n  -- Query 1: Daily Traffic Summary (first 10 rows) --\n");
                        sb.append(String.format("  %-14s %-8s %14s %14s%n",
                            "Date", "Status", "Requests", "Total Bytes"));
                        sb.append("  " + repeatChar('-', 54) + "\n");
                        hasRows = true;
                    }
                    sb.append(String.format("  %-14s %-8d %14d %14d%n",
                        rs.getString("log_date"), rs.getInt("status_code"),
                        rs.getLong("request_count"), rs.getLong("total_bytes")));
                }
                if (hasRows) System.out.print(sb);
            }
        }

        // Query 2 results
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT resource_path, request_count, total_bytes, distinct_host_count " +
                "FROM query2_results WHERE run_id = ? ORDER BY request_count DESC LIMIT 10")) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                boolean hasRows = false;
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    if (!hasRows) {
                        sb.append("\n  -- Query 2: Top Requested Resources (first 10) ----\n");
                        sb.append(String.format("  %-40s %10s %12s %8s%n",
                            "Resource", "Requests", "Bytes", "Hosts"));
                        sb.append("  " + repeatChar('-', 74) + "\n");
                        hasRows = true;
                    }
                    String path = rs.getString("resource_path");
                    if (path.length() > 39) path = path.substring(0, 36) + "...";
                    sb.append(String.format("  %-40s %10d %12d %8d%n",
                        path, rs.getLong("request_count"),
                        rs.getLong("total_bytes"), rs.getLong("distinct_host_count")));
                }
                if (hasRows) System.out.print(sb);
            }
        }

        // Query 3 results
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT log_date, log_hour, error_request_count, total_request_count, " +
                "error_rate, distinct_error_hosts " +
                "FROM query3_results WHERE run_id = ? ORDER BY log_date, log_hour LIMIT 10")) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                boolean hasRows = false;
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    if (!hasRows) {
                        sb.append("\n  -- Query 3: Hourly Error Analysis (first 10 rows) --\n");
                        sb.append(String.format("  %-14s %5s %8s %8s %10s %8s%n",
                            "Date", "Hour", "Errors", "Total", "Error%", "Hosts"));
                        sb.append("  " + repeatChar('-', 58) + "\n");
                        hasRows = true;
                    }
                    sb.append(String.format("  %-14s %5d %8d %8d %9.2f%% %8d%n",
                        rs.getString("log_date"), rs.getInt("log_hour"),
                        rs.getLong("error_request_count"), rs.getLong("total_request_count"),
                        rs.getDouble("error_rate") * 100, rs.getLong("distinct_error_hosts")));
                }
                if (hasRows) System.out.print(sb);
            }
        }

        System.out.println();
        System.out.println("======================================================");
        System.out.println("  Report complete.");
        System.out.println("======================================================");
    }

    /**
     * Returns a string consisting of the given character repeated n times.
     * Replacement for String.repeat() which requires Java 11+.
     */
    private static String repeatChar(char c, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
