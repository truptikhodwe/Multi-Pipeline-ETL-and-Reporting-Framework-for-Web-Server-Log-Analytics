package com.etl.controller;

import com.etl.db.PostgresWriter;
import com.etl.model.ExecutionStats;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * PigPipeline – orchestrates ETL execution for a single query (or all queries)
 * using Apache Pig on HDFS.
 *
 * Accepts queryName and batchSize at construction time so the CLI can drive
 * different execution combinations without recompiling.
 */
public class PigPipeline {

    // ── HDFS paths ──────────────────────────────────────────────────────────
    private static final String INPUT = "/input/logs";
    private static final String OUTPUT = "/output";

    // ── Environment paths ───────────────────────────────────────────────────
    private static final String HADOOP_HOME = "/home/priyanshu-tiwari/hadoop";
    private static final String PIG_HOME = "/home/priyanshu-tiwari/hadoop/pig-0.18.0";
    private static final String JAR_PATH = "target/etl-pipeline-1.0.jar";

    // ── Pipeline identity ───────────────────────────────────────────────────
    private static final String PIPELINE_NAME = "Pig";

    // ── CLI-driven parameters ───────────────────────────────────────────────
    private final String queryName; // "Query1" | "Query2" | "Query3" | "All"
    private final int batchId; // 1 = July, 2 = August (or 0 = combined)

    public PigPipeline(String queryName, int batchId) {
        this.queryName = queryName;
        this.batchId = batchId;
    }

    public String getPipelineName() {
        return PIPELINE_NAME;
    }

    public String getQueryName() {
        return queryName;
    }



    public int getBatchId() {
        return batchId;
    }

    // ════════════════════════════════════════════════════════════════════════
    // PUBLIC: run()
    // ════════════════════════════════════════════════════════════════════════

    public ExecutionStats run() throws Exception {
        long start = System.currentTimeMillis();

        ExecutionStats stats = new ExecutionStats();

        // Count raw input records
        int totalRecords = countRecords();

        // Remove previous output
        runCommand("hdfs dfs -rm -r -f " + OUTPUT);

        // Determine which Pig script(s) to execute
        List<String> scriptsToRun = resolveScripts();

        for (String script : scriptsToRun) {
            runPig(script);
        }

        // Gather stats
        stats.totalRecords = totalRecords;
        stats.malformedRecords = totalRecords - countValidRecords();
        stats.totalBatches = (batchId == 0) ? 2 : 1;
        stats.avgBatchSize = (stats.totalBatches == 0)
            ? 0.0
            : (double) stats.totalRecords / stats.totalBatches;
        stats.runtime = System.currentTimeMillis() - start;

        return stats;
    }

    // ════════════════════════════════════════════════════════════════════════
    // PUBLIC: loadResultsToMySQL()
    // ════════════════════════════════════════════════════════════════════════

    public void loadResultsToPostgres(int runId) throws Exception {
        Timestamp executedAt = new Timestamp(System.currentTimeMillis());

        PostgresWriter writer = null;
        try {
            writer = new PostgresWriter();
            switch (queryName) {
                case "Query1":
                    loadQuery1(writer, runId, executedAt);
                    break;
                case "Query2":
                    loadQuery2(writer, runId, executedAt);
                    break;
                case "Query3":
                    loadQuery3(writer, runId, executedAt);
                    break;
                default:
                    // "All"
                    loadQuery1(writer, runId, executedAt);
                    loadQuery2(writer, runId, executedAt);
                    loadQuery3(writer, runId, executedAt);
                    break;
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PUBLIC: copyOutput()
    // Pulls HDFS output to a local ./output directory for inspection
    // ════════════════════════════════════════════════════════════════════════

    public void copyOutput() throws Exception {
        runCommand("rm -rf output");
        runCommand("mkdir -p output");

        if (queryName.equals("Query1") || queryName.equals("All")) {
            copyHdfsDir(OUTPUT + "/query1", "output");
        }
        if (queryName.equals("Query2") || queryName.equals("All")) {
            copyHdfsDir(OUTPUT + "/query2", "output");
        }
        if (queryName.equals("Query3") || queryName.equals("All")) {
            copyHdfsDir(OUTPUT + "/query3", "output");
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PRIVATE HELPERS
    // ════════════════════════════════════════════════════════════════════════

    /** Returns the list of .pig script paths to run for this queryName. */
    private List<String> resolveScripts() {
        List<String> scripts = new ArrayList<>();
        String base = "src/main/resources/pig/";

        switch (queryName) {
            case "Query1":
                scripts.add(base + "query1.pig");
                break;
            case "Query2":
                scripts.add(base + "query2.pig");
                break;
            case "Query3":
                scripts.add(base + "query3.pig");
                break;
            default:
                scripts.add(base + "etl.pig"); // "All" → combined script
                break;
        }
        return scripts;
    }

    private void runPig(String scriptPath) throws Exception {
        String cmd =
            "pig" +
            " -param INPUT=" +
            INPUT +
            " -param OUTPUT=" +
            OUTPUT +
            " " +
            scriptPath;
        runCommand(cmd);
    }

    private void runCommand(String command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);

        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb.environment().put("PIG_HOME", PIG_HOME);
        pb.environment().put("HADOOP_CLASSPATH", JAR_PATH);
        pb
            .environment()
            .put(
                "PATH",
                pb.environment().get("PATH") +
                    ":" +
                    HADOOP_HOME +
                    "/bin" +
                    ":" +
                    HADOOP_HOME +
                    "/sbin" +
                    ":" +
                    PIG_HOME +
                    "/bin"
            );

        pb.redirectErrorStream(true);
        Process p = pb.start();

        try (
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream())
            )
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }

        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException(
                "Command failed (exit=" + exitCode + "): " + command
            );
        }
    }

    private void copyHdfsDir(String hdfsPath, String localParentDir)
        throws Exception {
        runCommand("hdfs dfs -test -e " + hdfsPath);
        runCommand("hdfs dfs -get " + hdfsPath + " " + localParentDir);
    }

    // ── Record counters ──────────────────────────────────────────────────────

    private int countRecords() throws Exception {
        return runCountCommand(
            "hdfs dfs -cat " + INPUT + "/* 2>/dev/null | wc -l"
        );
    }

    private int countValidRecords() throws Exception {
        return runCountCommand(
            "hdfs dfs -cat " + OUTPUT + "/valid_logs/* 2>/dev/null | wc -l"
        );
    }

    private int runCountCommand(String cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb
            .environment()
            .put(
                "PATH",
                pb.environment().get("PATH") +
                    ":" +
                    HADOOP_HOME +
                    "/bin" +
                    ":" +
                    HADOOP_HOME +
                    "/sbin"
            );
        Process p = pb.start();

        String output;
        try (
            BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream())
            )
        ) {
            output = r.readLine();
        }

        int exit = p.waitFor();
        if (exit != 0) throw new RuntimeException(
            "Count command failed: " + cmd
        );

        return (output == null || output.trim().isEmpty())
            ? 0
            : Integer.parseInt(output.trim());
    }

    // ── Result loaders ───────────────────────────────────────────────────────

    private void loadQuery1(
        PostgresWriter writer,
        int runId,
        Timestamp executedAt
    ) throws Exception {
        for (String line : readHdfsLines(OUTPUT + "/query1/part-*")) {
            if (line == null || line.trim().isEmpty()) continue;

            String[] p = line.split("\t", -1);
            if (p.length != 5) {
                System.err.println("Skipping malformed Query1 row: " + line);
                continue;
            }

            writer.insertQuery1(
                runId,
                PIPELINE_NAME,
                Integer.parseInt(p[0]), // batch_id
                executedAt,
                p[1], // log_date
                Integer.parseInt(p[2]), // status_code
                Long.parseLong(p[3]), // request_count
                Long.parseLong(p[4]) // total_bytes
            );
        }
    }

    private void loadQuery2(
        PostgresWriter writer,
        int runId,
        Timestamp executedAt
    ) throws Exception {
        for (String line : readHdfsLines(OUTPUT + "/query2/part-*")) {
            if (line == null || line.trim().isEmpty()) continue;

            String[] p = line.split("\t", -1);
            if (p.length != 5) {
                System.err.println("Skipping malformed Query2 row: " + line);
                continue;
            }

            writer.insertQuery2(
                runId,
                PIPELINE_NAME,
                Integer.parseInt(p[0]), // batch_id
                executedAt,
                p[1], // resource_path
                Long.parseLong(p[2]), // request_count
                Long.parseLong(p[3]), // total_bytes
                Long.parseLong(p[4]) // distinct_host_count
            );
        }
    }

    private void loadQuery3(
        PostgresWriter writer,
        int runId,
        Timestamp executedAt
    ) throws Exception {
        for (String line : readHdfsLines(OUTPUT + "/query3/part-*")) {
            if (line == null || line.trim().isEmpty()) continue;

            String[] p = line.split("\t", -1);
            if (p.length != 7) {
                System.err.println("Skipping malformed Query3 row: " + line);
                continue;
            }

            writer.insertQuery3(
                runId,
                PIPELINE_NAME,
                Integer.parseInt(p[0]), // batch_id
                executedAt,
                p[1], // log_date
                Integer.parseInt(p[2]), // log_hour
                Long.parseLong(p[3]), // error_request_count
                Long.parseLong(p[4]), // total_request_count
                Double.parseDouble(p[5]), // error_rate
                Long.parseLong(p[6]) // distinct_error_hosts
            );
        }
    }

    private List<String> readHdfsLines(String hdfsGlob) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
            "bash",
            "-c",
            "hdfs dfs -cat " + shellQuote(hdfsGlob)
        );
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb
            .environment()
            .put(
                "PATH",
                pb.environment().get("PATH") +
                    ":" +
                    HADOOP_HOME +
                    "/bin" +
                    ":" +
                    HADOOP_HOME +
                    "/sbin"
            );
        pb.redirectErrorStream(true);

        Process process = pb.start();
        List<String> lines = new ArrayList<>();

        try (
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream())
            )
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Failed to read HDFS path: " + hdfsGlob);
        }
        return lines;
    }

    private String shellQuote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }
}
