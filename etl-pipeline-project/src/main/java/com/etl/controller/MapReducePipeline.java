package com.etl.controller;

import com.etl.db.PostgresWriter;
import com.etl.model.ExecutionStats;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.*;

/**
 * MapReducePipeline – runs ETL as a chain of MapReduce jobs:
 *  1. CleanerJob  : raw logs → /output/valid_logs
 *  2. QueryN Job  : valid_logs → /output/queryN
 */
public class MapReducePipeline {

    private static final String INPUT = "/input/logs";
    private static final String OUTPUT = "/output";
    private static final String PIPELINE = "MapReduce";
    private static final String HADOOP_HOME = "/home/priyanshu-tiwari/hadoop";
    private static final String JAR_PATH = "target/etl-pipeline-1.0.jar"; // fat JAR for HADOOP_CLASSPATH
    private static final String MR_JAR_PATH = "target/etl-pipeline-mr.jar"; // no Main-Class, used for hadoop jar

    private final String queryName;
    private final int batchSize;
    private final int batchId;

    public MapReducePipeline(String queryName, int batchSize, int batchId) {
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

    // ── Main execution ────────────────────────────────────────────────────────
    public ExecutionStats run() throws Exception {
        long start = System.currentTimeMillis();

        int totalRecords = countRecords(INPUT + "/*");
        runCommand("hdfs dfs -rm -r -f " + OUTPUT);

        // Step 1: Clean / load (mandatory MR job for ETL)
        runHadoopJob(
            "com.etl.mapreduce.CleanerMapper",
            "com.etl.mapreduce.CleanerReducer",
            INPUT,
            OUTPUT + "/valid_logs"
        );

        // Step 2: Selected query job(s)
        if (queryName.equals("Query1") || queryName.equals("All")) runHadoopJob(
            "com.etl.mapreduce.Query1Mapper",
            "com.etl.mapreduce.Query1Reducer",
            OUTPUT + "/valid_logs",
            OUTPUT + "/query1"
        );

        if (queryName.equals("Query2") || queryName.equals("All")) runHadoopJob(
            "com.etl.mapreduce.Query2Mapper",
            "com.etl.mapreduce.Query2Reducer",
            OUTPUT + "/valid_logs",
            OUTPUT + "/query2"
        );

        if (queryName.equals("Query3") || queryName.equals("All")) runHadoopJob(
            "com.etl.mapreduce.Query3Mapper",
            "com.etl.mapreduce.Query3Reducer",
            OUTPUT + "/valid_logs",
            OUTPUT + "/query3"
        );

        int validRecords = countRecords(OUTPUT + "/valid_logs/*");
        int malformed = totalRecords - validRecords;
        int totalBatches =
            totalRecords == 0
                ? 0
                : (int) Math.ceil((double) totalRecords / batchSize);
        double avgBatch =
            totalBatches == 0 ? 0.0 : (double) totalRecords / totalBatches;

        ExecutionStats s = new ExecutionStats();
        s.totalRecords = totalRecords;
        s.malformedRecords = malformed;
        s.totalBatches = totalBatches;
        s.avgBatchSize = avgBatch;
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

    // ── Hadoop job runner ─────────────────────────────────────────────────────
    private void runHadoopJob(
        String mapperClass,
        String reducerClass,
        String input,
        String output
    ) throws Exception {
        runCommand(
            String.format(
                "hadoop jar %s com.etl.mapreduce.HadoopRunner %s %s %s %s",
                MR_JAR_PATH,
                mapperClass,
                reducerClass,
                input,
                output
            )
        );
    }

    // ── Result loaders ────────────────────────────────────────────────────────
    private void loadQuery1(PostgresWriter w, int runId, Timestamp ts)
        throws Exception {
        for (String line : readHdfsLines(OUTPUT + "/query1/part-*")) {
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
        // Sort by count desc, take top 20
        List<String[]> rows = new ArrayList<>();
        for (String line : readHdfsLines(OUTPUT + "/query2/part-*")) {
            if (line.trim().isEmpty()) continue;
            String[] p = line.split("\t", -1);
            if (p.length >= 5) rows.add(p);
        }
        rows.sort((a, b) ->
            Long.compare(Long.parseLong(b[2]), Long.parseLong(a[2]))
        );
        for (String[] p : rows.subList(0, Math.min(20, rows.size()))) {
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
        for (String line : readHdfsLines(OUTPUT + "/query3/part-*")) {
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

    // ── Utilities ─────────────────────────────────────────────────────────────
    private void runCommand(String command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb.environment().put("HADOOP_CLASSPATH", JAR_PATH);
        pb.environment().put("JAVA_HOME", System.getProperty("java.home"));
        pb
            .environment()
            .put(
                "PATH",
                pb.environment().get("PATH") +
                    ":" +
                    HADOOP_HOME +
                    "/bin:" +
                    HADOOP_HOME +
                    "/sbin"
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
            "Command failed (exit=" + exit + "): " + command
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
                pb.environment().get("PATH") +
                    ":" +
                    HADOOP_HOME +
                    "/bin:" +
                    HADOOP_HOME +
                    "/sbin"
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

    private List<String> readHdfsLines(String hdfsGlob) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
            "bash",
            "-c",
            "hdfs dfs -cat '" + hdfsGlob.replace("'", "'\\''") + "'"
        );
        pb.environment().put("HADOOP_HOME", HADOOP_HOME);
        pb
            .environment()
            .put(
                "PATH",
                pb.environment().get("PATH") +
                    ":" +
                    HADOOP_HOME +
                    "/bin:" +
                    HADOOP_HOME +
                    "/sbin"
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
