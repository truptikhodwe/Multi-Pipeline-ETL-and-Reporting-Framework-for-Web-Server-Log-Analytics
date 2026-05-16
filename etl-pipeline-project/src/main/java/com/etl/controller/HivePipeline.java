package com.etl.controller;

import com.etl.db.PostgresWriter;
import com.etl.model.ExecutionStats;
import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * HivePipeline – runs ETL queries via beeline (HiveServer2) or the hive CLI.
 * Execution order:
 *  1. setup.hql  – creates external table + valid_logs ORC table (load + clean)
 *  2. queryN.hql – writes results to HDFS /output/queryN
 *  3. Reads HDFS output → loads into PostgreSQL
 */
public class HivePipeline {

    private static final String OUTPUT = "/output";
    private static final String PIPELINE = "Hive";
    private static final String HADOOP_HOME = "/usr/local/hadoop";
    private static final String HIVE_HOME = "/home/siddharth-kini/hive";
    private static final String HQL_BASE = "src/main/resources/hive/";

    private final String queryName;
    private final int batchSize;
    private final int batchId;

    public HivePipeline(String queryName, int batchSize, int batchId) {
        this.queryName = queryName;
        this.batchSize = batchSize;
        this.batchId = batchId;
    }

    public String getPipelineName() {
        return PIPELINE;
    }

    public String getQueryName() {
        return queryName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchId() {
        return batchId;
    }

    public ExecutionStats run() throws Exception {
        long start = System.currentTimeMillis();

        // Remove previous output
        runCommand(
            "hdfs dfs -rm -r -f " +
                OUTPUT +
                "/query1 " +
                OUTPUT +
                "/query2 " +
                OUTPUT +
                "/query3"
        );

        // Step 1: Setup (load + clean)
        runHive(HQL_BASE + "setup.hql");

        // Step 2: Selected query / queries
        if (queryName.equals("Query1") || queryName.equals("All")) runHive(
            HQL_BASE + "query1.hql"
        );
        if (queryName.equals("Query2") || queryName.equals("All")) runHive(
            HQL_BASE + "query2.hql"
        );
        if (queryName.equals("Query3") || queryName.equals("All")) runHive(
            HQL_BASE + "query3.hql"
        );

        int totalRecords = countRecords("/input/logs/*");
        // valid_logs is a Hive-managed ORC table stored in the Hive warehouse, not under /output.
        // Use the Hive metastore default warehouse path: /user/hive/warehouse/etl_logs.db/valid_logs
        int validRecords = countHiveRecords("etl_logs.valid_logs");

        ExecutionStats s = new ExecutionStats();
        s.totalRecords = totalRecords;
        s.malformedRecords = totalRecords - validRecords;
        s.totalBatches =
            totalRecords == 0
                ? 0
                : (int) Math.ceil((double) totalRecords / batchSize);
        s.avgBatchSize =
            s.totalBatches == 0 ? 0.0 : (double) totalRecords / s.totalBatches;
        s.runtime = System.currentTimeMillis() - start;
        return s;
    }

    public void loadResultsToPostgres(int runId) throws Exception {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        try (PostgresWriter w = new PostgresWriter()) {
            if (
                queryName.equals("Query1") || queryName.equals("All")
            ) loadQuery1(w, runId, ts);
            if (
                queryName.equals("Query2") || queryName.equals("All")
            ) loadQuery2(w, runId, ts);
            if (
                queryName.equals("Query3") || queryName.equals("All")
            ) loadQuery3(w, runId, ts);
        }
    }

    private void runHive(String hqlFile) throws Exception {
        runCommand("hive -f " + hqlFile);
    }

    private void loadQuery1(PostgresWriter w, int runId, Timestamp ts)
        throws Exception {
        for (String line : readHdfsLines(OUTPUT + "/query1/000000_0")) {
            if (line.trim().isEmpty()) continue;
            String[] p = line.split("\t", -1);
            if (p.length < 5) continue;
            w.insertQuery1(
                runId,
                PIPELINE,
                Integer.parseInt(p[0]),
                ts,
                p[1],
                Integer.parseInt(p[2]),
                Long.parseLong(p[3]),
                Long.parseLong(p[4])
            );
        }
    }

    private void loadQuery2(PostgresWriter w, int runId, Timestamp ts)
        throws Exception {
        for (String line : readHdfsLines(OUTPUT + "/query2/000000_0")) {
            if (line.trim().isEmpty()) continue;
            String[] p = line.split("\t", -1);
            if (p.length < 5) continue;
            w.insertQuery2(
                runId,
                PIPELINE,
                Integer.parseInt(p[0]),
                ts,
                p[1],
                Long.parseLong(p[2]),
                Long.parseLong(p[3]),
                Long.parseLong(p[4])
            );
        }
    }

    private void loadQuery3(PostgresWriter w, int runId, Timestamp ts)
        throws Exception {
        for (String line : readHdfsLines(OUTPUT + "/query3/000000_0")) {
            if (line.trim().isEmpty()) continue;
            String[] p = line.split("\t", -1);
            if (p.length < 7) continue;
            w.insertQuery3(
                runId,
                PIPELINE,
                Integer.parseInt(p[0]),
                ts,
                p[1],
                Integer.parseInt(p[2]),
                Long.parseLong(p[3]),
                Long.parseLong(p[4]),
                Double.parseDouble(p[5]),
                Long.parseLong(p[6])
            );
        }
    }

    private void runCommand(String command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb.environment().put("HIVE_HOME", HIVE_HOME);
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
                    HIVE_HOME +
                    "/bin"
            );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        try (
            BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream())
            )
        ) {
            String l;
            while ((l = r.readLine()) != null) System.out.println(l);
        }
        int exit = p.waitFor();
        if (exit != 0) throw new RuntimeException(
            "Hive command failed: " + command
        );
    }

    private int countRecords(String hdfsGlob) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
            "bash",
            "-c",
            "hdfs dfs -cat " + hdfsGlob + " 2>/dev/null | wc -l"
        );
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb
            .environment()
            .put(
                "PATH",
                pb.environment().get("PATH") + ":" + HADOOP_HOME + "/bin"
            );
        Process p = pb.start();
        String out;
        try (
            BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream())
            )
        ) {
            out = r.readLine();
        }
        p.waitFor();
        return (out == null || out.trim().isEmpty())
            ? 0
            : Integer.parseInt(out.trim());
    }

    private int countHiveRecords(String table) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
            "bash",
            "-c",
            "hive -S -e 'SELECT COUNT(1) FROM " + table + ";'"
        );
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb.environment().put("HIVE_HOME", HIVE_HOME);
        pb.environment().put(
            "PATH",
            pb.environment().get("PATH") + ":" + HADOOP_HOME + "/bin:" + HIVE_HOME + "/bin"
        );
        
        Process p = pb.start();
        String out = "";
        
        try (
            BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream())
            )
        ) {
            String line;
            while ((line = r.readLine()) != null) {
                // The -S (silent) flag might still output some warnings, 
                // so we parse only lines containing digits.
                if (line.trim().matches("\\d+")) {
                    out = line.trim();
                }
            }
        }
        p.waitFor();
        return out.isEmpty() ? 0 : Integer.parseInt(out);
    }

    private List<String> readHdfsLines(String hdfsPath) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
            "bash",
            "-c",
            "hdfs dfs -cat " + hdfsPath
        );
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb
            .environment()
            .put(
                "PATH",
                pb.environment().get("PATH") + ":" + HADOOP_HOME + "/bin"
            );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        List<String> lines = new ArrayList<>();
        try (
            BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream())
            )
        ) {
            String l;
            while ((l = r.readLine()) != null) lines.add(l);
        }
        p.waitFor();
        return lines;
    }
}
