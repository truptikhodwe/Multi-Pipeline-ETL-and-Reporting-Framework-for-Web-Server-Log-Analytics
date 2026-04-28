package com.etl.controller;

import com.etl.model.ExecutionStats;
import com.etl.db.PostgresWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.List;

import java.io.*;
import java.util.ArrayList;

public class PigPipeline {

    private static final String INPUT = "/input/logs";
    private static final String OUTPUT = "/output";
    private static final int BATCH_SIZE = 1000;
    private static final String PIPELINE_NAME = "Pig";

    public int getBatchSize() {
        return BATCH_SIZE;
    }

    public String getPipelineName() {
        return PIPELINE_NAME;
    }

    public ExecutionStats run() throws Exception {
        long start = System.currentTimeMillis();

        ExecutionStats stats = new ExecutionStats();

        int totalRecords = countRecords();

        runCommand("hdfs dfs -rm -r -f " + OUTPUT);

        runCommand(
            "pig " +
            "-param INPUT=" + INPUT + " " +
            "-param OUTPUT=" + OUTPUT + " " +
            "-param BATCH_SIZE=" + BATCH_SIZE + " " +
            "src/main/resources/pig/etl.pig"
        );

        stats.totalRecords = totalRecords;
        stats.malformedRecords = totalRecords - countValidRecords();
        stats.totalBatches = totalRecords == 0
            ? 0
            : (int) Math.ceil((double) totalRecords / BATCH_SIZE);

        stats.avgBatchSize = stats.totalBatches == 0
            ? 0.0
            : (double) stats.totalRecords / stats.totalBatches;

        stats.runtime = System.currentTimeMillis() - start;

        return stats;
    }

    private void runCommand(String command) throws Exception {

        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);

        pb.environment().put("HADOOP_HOME", "/home/priyanshu-tiwari/hadoop");
        pb.environment().put("PIG_HOME", "/home/priyanshu-tiwari/hadoop/pig-0.18.0");

        pb.environment().put("PATH",
                pb.environment().get("PATH") +
                        ":/home/priyanshu-tiwari/hadoop/bin" +
                        ":/home/priyanshu-tiwari/hadoop/sbin" +
                        ":/home/priyanshu-tiwari/hadoop/pig-0.18.0/bin"
        );

        pb.redirectErrorStream(true);

        Process p = pb.start();

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream())
        );

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        int exitCode = p.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Command failed: " + command);
        }
    }

public void loadResultsToPostgres(int runId) throws Exception {
    Timestamp executedAt = new Timestamp(System.currentTimeMillis());

    try (PostgresWriter writer = new PostgresWriter()) {
        loadQuery1(writer, runId, executedAt);
        loadQuery2(writer, runId, executedAt);
        loadQuery3(writer, runId, executedAt);
    }
}

private void loadQuery1(PostgresWriter writer, int runId, Timestamp executedAt) throws Exception {
    int resultBatchId = 0;

    for (String line : readHdfsLines(OUTPUT + "/query1/part-*")) {
        if (line == null || line.isBlank()) {
            continue;
        }

        String[] parts = line.split("\\t", -1);

        if (parts.length != 5) {
            throw new IllegalStateException("Invalid Query 1 output row: " + line);
        }

        int batchId = Integer.parseInt(parts[0]);
        String logDate = parts[1];
        int status = Integer.parseInt(parts[2]);
        long requestCount = Long.parseLong(parts[3]);
        long totalBytes = Long.parseLong(parts[4]);

        writer.insertQuery1(
                runId,
                PIPELINE_NAME,
                batchId,
                executedAt,
                logDate,
                status,
                requestCount,
                totalBytes
        );
    }
}

private void loadQuery2(PostgresWriter writer, int runId, Timestamp executedAt) throws Exception {
    for (String line : readHdfsLines(OUTPUT + "/query2/part-*")) {
        if (line == null || line.isBlank()) {
            continue;
        }

        String[] parts = line.split("\t", -1);

        if (parts.length != 5) {
            throw new IllegalStateException("Invalid Query 2 output row: " + line);
        }

        int batchId = Integer.parseInt(parts[0]);
        String path = parts[1];
        long requestCount = Long.parseLong(parts[2]);
        long totalBytes = Long.parseLong(parts[3]);
        long distinctHostCount = Long.parseLong(parts[4]);

        writer.insertQuery2(
                runId,
                PIPELINE_NAME,
                batchId,
                executedAt,
                path,
                requestCount,
                totalBytes,
                distinctHostCount
        );
    }
}

private void loadQuery3(PostgresWriter writer, int runId, Timestamp executedAt) throws Exception {
    for (String line : readHdfsLines(OUTPUT + "/query3/part-*")) {
        if (line == null || line.isBlank()) {
            continue;
        }

        String[] parts = line.split("\t", -1);

        if (parts.length != 7) {
            throw new IllegalStateException("Invalid Query 3 output row: " + line);
        }

        int batchId = Integer.parseInt(parts[0]);
        String logDate = parts[1];
        int logHour = Integer.parseInt(parts[2]);
        long errorRequestCount = Long.parseLong(parts[3]);
        long totalRequestCount = Long.parseLong(parts[4]);
        double errorRate = Double.parseDouble(parts[5]);
        long distinctErrorHosts = Long.parseLong(parts[6]);

        writer.insertQuery3(
                runId,
                PIPELINE_NAME,
                batchId,
                executedAt,
                logDate,
                logHour,
                errorRequestCount,
                totalRequestCount,
                errorRate,
                distinctErrorHosts
        );
    }
}

public void copyOutput() throws Exception {
    runCommand("rm -rf output");
    runCommand("mkdir -p output");

    copyRequiredOutputDirectory("/output/query1", "output");
    copyRequiredOutputDirectory("/output/query2", "output");
    copyRequiredOutputDirectory("/output/query3", "output");
}

private void copyRequiredOutputDirectory(String hdfsPath, String localParentDir) throws Exception {
    runCommand("hdfs dfs -test -e " + hdfsPath);
    runCommand("hdfs dfs -get " + hdfsPath + " " + localParentDir);
}

private int countRecords() throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder(
            "bash",
            "-c",
            "hdfs dfs -cat " + INPUT + "/* 2>/dev/null | wc -l"
    );

    Process process = processBuilder.start();

    String output;
    try (java.io.BufferedReader reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(process.getInputStream())
    )) {
        output = reader.readLine();
    }

    int exitCode = process.waitFor();
    if (exitCode != 0) {
        throw new RuntimeException("Failed to count records in HDFS input path: " + INPUT);
    }

    if (output == null || output.isBlank()) {
        return 0;
    }

    return Integer.parseInt(output.trim());
}

private int countValidRecords() throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder(
            "bash",
            "-c",
            "hdfs dfs -cat " + OUTPUT + "/valid_logs/* 2>/dev/null | wc -l"
    );

    Process process = processBuilder.start();

    String output;
    try (java.io.BufferedReader reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(process.getInputStream())
    )) {
        output = reader.readLine();
    }

    int exitCode = process.waitFor();
    if (exitCode != 0) {
        throw new RuntimeException("Failed to count records in HDFS input path: " + INPUT);
    }

    if (output == null || output.isBlank()) {
        return 0;
    }

    return Integer.parseInt(output.trim());
}

private List<String> readHdfsLines(String hdfsPath) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
            "bash",
            "-c",
            "hdfs dfs -cat " + shellQuote(hdfsPath)
    );

    pb.redirectErrorStream(true);

    Process process = pb.start();

    List<String> lines = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getInputStream()))) {

        String line;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
    }

    int exitCode = process.waitFor();

    if (exitCode != 0) {
        throw new RuntimeException("Failed to read HDFS file: " + hdfsPath);
    }

    return lines;
}

private String shellQuote(String value) {
    return "'" + value.replace("'", "'\\''") + "'";
}
}