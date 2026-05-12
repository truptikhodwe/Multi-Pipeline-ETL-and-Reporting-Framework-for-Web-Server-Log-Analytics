package com.etl;

import com.etl.controller.*;
import com.etl.db.PostgresWriter;
import com.etl.model.ExecutionStats;
import com.etl.report.ReportGenerator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Scanner;

/**
 * Main – unified CLI entry point for the Multi-Pipeline ETL Framework.
 *
 * Workflow:
 *   1. Select pipeline  (Pig / MapReduce / Hive / MongoDB)
 *   2. Select query     (1 / 2 / 3 / All)
 *   3. Select batch     (July / August / Combined)
 *   4. Set batch size
 *   5. Execute pipeline
 *   6. Load results into PostgreSQL
 *   7. Generate and print report
 */
public class Main {

    private static final String PG_URL =
        "jdbc:postgresql://localhost:5432/etl_project";
    private static final String PG_USER = "postgres";
    private static final String PG_PASS = "postgres";

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        printBanner();

        // ── Step 1: Pipeline ──────────────────────────────────────────────────
        System.out.println("┌─────────────────────────────────────────────┐");
        System.out.println("│           SELECT EXECUTION PIPELINE          │");
        System.out.println("├─────────────────────────────────────────────┤");
        System.out.println("│  1. Pig                                      │");
        System.out.println("│  2. MapReduce                                │");
        System.out.println("│  3. Hive                                     │");
        System.out.println("│  4. MongoDB                                  │");
        System.out.println("└─────────────────────────────────────────────┘");
        System.out.print("Enter choice [1-4]: ");
        int pipelineChoice = readInt(sc, 1, 4);

        String pipelineName = switch (pipelineChoice) {
            case 1 -> "Pig";
            case 2 -> "MapReduce";
            case 3 -> "Hive";
            default -> "MongoDB";
        };
        System.out.println("→ Pipeline: " + pipelineName + "\n");

        // ── Step 2: Query ─────────────────────────────────────────────────────
        System.out.println("┌─────────────────────────────────────────────┐");
        System.out.println("│               SELECT QUERY                   │");
        System.out.println("├─────────────────────────────────────────────┤");
        System.out.println("│  1. Query 1 – Daily Traffic Summary          │");
        System.out.println("│  2. Query 2 – Top 20 Requested Resources     │");
        System.out.println("│  3. Query 3 – Hourly Error Analysis          │");
        System.out.println("│  4. All Queries                              │");
        System.out.println("└─────────────────────────────────────────────┘");
        System.out.print("Enter choice [1-4]: ");
        int queryChoice = readInt(sc, 1, 4);
        String queryName = switch (queryChoice) {
            case 1 -> "Query1";
            case 2 -> "Query2";
            case 3 -> "Query3";
            default -> "All";
        };
        System.out.println("→ Query: " + queryName + "\n");

        // ── Step 3: Batch ─────────────────────────────────────────────────────
        System.out.println("┌─────────────────────────────────────────────┐");
        System.out.println("│             SELECT BATCH / DATASET           │");
        System.out.println("├─────────────────────────────────────────────┤");
        System.out.println("│  1. Batch 1 – July   1995 logs               │");
        System.out.println("│  2. Batch 2 – August 1995 logs               │");
        System.out.println("│  3. All batches (combined)                   │");
        System.out.println("└─────────────────────────────────────────────┘");
        System.out.print("Enter choice [1-3]: ");
        int batchChoice = readInt(sc, 1, 3);
        int batchId = batchChoice <= 2 ? batchChoice : 0;
        String batchLabel =
            batchId == 1
                ? "July 1995"
                : batchId == 2
                    ? "August 1995"
                    : "Combined";
        System.out.println("→ Batch: " + batchLabel + "\n");

        // ── Step 4: Batch size ────────────────────────────────────────────────
        System.out.print("Enter batch size [default: 1000]: ");
        String bsRaw = sc.nextLine().trim();
        int batchSize = bsRaw.isEmpty() ? 1000 : Integer.parseInt(bsRaw);
        System.out.println("→ Batch size: " + batchSize + "\n");

        // ── Confirm ───────────────────────────────────────────────────────────
        System.out.println("─".repeat(48));
        System.out.printf("  Pipeline : %s%n", pipelineName);
        System.out.printf("  Query    : %s%n", queryName);
        System.out.printf("  Batch    : %s%n", batchLabel);
        System.out.printf("  Batch Sz : %d%n", batchSize);
        System.out.println("─".repeat(48));
        System.out.print("Proceed? [Y/n]: ");
        String confirm = sc.nextLine().trim();
        if (!confirm.isEmpty() && !confirm.equalsIgnoreCase("y")) {
            System.out.println("Aborted.");
            return;
        }

        // ── Step 5: Execute ───────────────────────────────────────────────────
        ExecutionStats stats = new ExecutionStats();
        int runId = -1;
        int effectiveBatchId = batchId == 0 ? 1 : batchId;
        String sourceFile =
            batchId == 1
                ? "/input/logs/NASA_access_log_Jul95"
                : batchId == 2
                    ? "/input/logs/NASA_access_log_Aug95"
                    : "/input/logs";

        // Pre-flight: verify HDFS is reachable (all pipelines read logs from HDFS)
        if (true) {
            System.out.println("\n[Preflight] Checking HDFS connectivity...");
            if (!isHdfsReachable()) {
                System.out.println();
                System.out.println(
                    "╔══════════════════════════════════════════════════════╗"
                );
                System.out.println(
                    "║  ERROR: Hadoop is not running!                       ║"
                );
                System.out.println(
                    "║  Please start Hadoop first:                          ║"
                );
                System.out.println(
                    "║    start-dfs.sh && start-yarn.sh                     ║"
                );
                System.out.println(
                    "║  Then re-run this program.                           ║"
                );
                System.out.println(
                    "╚══════════════════════════════════════════════════════╝"
                );
                return;
            }
            System.out.println("[Preflight] HDFS is reachable. ✓\n");
        }

        System.out.println("\n[1/4] Running " + pipelineName + " pipeline...");
        try {
            switch (pipelineName) {
                case "Pig" -> {
                    PigPipeline pig = new PigPipeline(
                        queryName,
                        batchSize,
                        batchId
                    );
                    stats = pig.run();
                    System.out.println("\n[2/4] Copying HDFS output...");
                    pig.copyOutput();
                    System.out.println(
                        "\n[3/4] Loading results into PostgreSQL..."
                    );
                    runId = insertRunMeta(
                        pipelineName,
                        queryName,
                        effectiveBatchId,
                        batchSize,
                        stats,
                        batchLabel,
                        sourceFile
                    );
                    pig.loadResultsToPostgres(runId);
                }
                case "MapReduce" -> {
                    MapReducePipeline mr = new MapReducePipeline(
                        queryName,
                        batchSize,
                        batchId
                    );
                    stats = mr.run();
                    System.out.println(
                        "\n[2/4] Skipping local copy (results read from HDFS)..."
                    );
                    System.out.println(
                        "\n[3/4] Loading results into PostgreSQL..."
                    );
                    runId = insertRunMeta(
                        pipelineName,
                        queryName,
                        effectiveBatchId,
                        batchSize,
                        stats,
                        batchLabel,
                        sourceFile
                    );
                    mr.loadResultsToPostgres(runId);
                }
                case "Hive" -> {
                    HivePipeline hive = new HivePipeline(
                        queryName,
                        batchSize,
                        batchId
                    );
                    stats = hive.run();
                    System.out.println(
                        "\n[2/4] Skipping local copy (results read from HDFS)..."
                    );
                    System.out.println(
                        "\n[3/4] Loading results into PostgreSQL..."
                    );
                    runId = insertRunMeta(
                        pipelineName,
                        queryName,
                        effectiveBatchId,
                        batchSize,
                        stats,
                        batchLabel,
                        sourceFile
                    );
                    hive.loadResultsToPostgres(runId);
                }
                default -> {
                    // MongoDB
                    MongoDBPipeline mongo = new MongoDBPipeline(
                        queryName,
                        batchSize,
                        batchId
                    );
                    stats = mongo.run();
                    System.out.println(
                        "\n[2/4] Results stored in MongoDB collections..."
                    );
                    System.out.println(
                        "\n[3/4] Loading results into PostgreSQL..."
                    );
                    runId = insertRunMeta(
                        pipelineName,
                        queryName,
                        effectiveBatchId,
                        batchSize,
                        stats,
                        batchLabel,
                        sourceFile
                    );
                    mongo.loadResultsToPostgres(runId);
                }
            }
        } catch (Exception ex) {
            System.err.println("\n[ERROR] Pipeline execution failed:");
            System.err.println("  " + ex.getMessage());
            System.err.println(
                "\nTip: Check that Hadoop / Hive / MongoDB services are running."
            );
            return;
        }

        // ── Step 6: Report ────────────────────────────────────────────────────
        System.out.println("\n[4/4] Generating report...");
        try (
            Connection conn = DriverManager.getConnection(
                PG_URL,
                PG_USER,
                PG_PASS
            )
        ) {
            ReportGenerator.printReport(runId, conn);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static int insertRunMeta(
        String pipeline,
        String query,
        int batchId,
        int batchSize,
        ExecutionStats stats,
        String batchLabel,
        String sourceFile
    ) throws Exception {
        try (PostgresWriter w = new PostgresWriter()) {
            int runId = w.insertRun(
                pipeline,
                query,
                batchId,
                batchSize,
                stats.avgBatchSize,
                stats.totalRecords,
                stats.malformedRecords,
                stats.runtime
            );
            w.insertBatchMetadata(
                runId,
                batchId,
                batchLabel,
                sourceFile,
                stats.totalRecords
            );
            w.insertMalformedSummary(
                runId,
                batchId,
                stats.malformedRecords,
                stats.totalRecords
            );
            System.out.println("  → Run ID: " + runId);
            return runId;
        }
    }

    private static void printBanner() {
        System.out.println(
            """
            ╔══════════════════════════════════════════════════════╗
            ║       Multi-Pipeline ETL & Reporting Framework       ║
            ║           End Semester Project – NoSQL Systems       ║
            ╚══════════════════════════════════════════════════════╝
            """
        );
    }

    private static int readInt(Scanner sc, int min, int max) {
        while (true) {
            try {
                int val = Integer.parseInt(sc.nextLine().trim());
                if (val >= min && val <= max) return val;
            } catch (NumberFormatException ignored) {}
            System.out.printf(
                "Please enter a number between %d and %d: ",
                min,
                max
            );
        }
    }

    /**
     * Checks HDFS connectivity by running a lightweight dfsadmin command.
     * Returns true if HDFS is up, false if connection is refused.
     */
    private static boolean isHdfsReachable() {
        String hadoopHome = "/usr/local/hadoop";
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "bash",
                "-c",
                "hdfs dfs -ls / > /dev/null 2>&1"
            );
            pb.environment().put("HADOOP_HOME", hadoopHome);
            pb
                .environment()
                .put(
                    "PATH",
                    pb.environment().get("PATH") +
                        ":" +
                        hadoopHome +
                        "/bin:" +
                        hadoopHome +
                        "/sbin"
                );
            Process p = pb.start();
            // Wait max 8 seconds for the health check
            boolean finished = p.waitFor(
                8,
                java.util.concurrent.TimeUnit.SECONDS
            );
            if (!finished) {
                p.destroyForcibly();
                return false;
            }
            return p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
