package com.etl.controller;

import com.etl.db.PostgresWriter;
import com.etl.model.ExecutionStats;
import com.mongodb.client.*;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.*;
import org.bson.Document;

/**
 * MongoDBPipeline – ETL using MongoDB as the processing engine.
 *
 * Steps:
 *  1. Stream raw HDFS logs → parse with Java regex → bulk-insert into raw_logs collection
 *  2. Run MongoDB aggregation pipeline (clean.js equivalent) → valid_logs collection
 *  3. Run selected query .js file via mongosh → capture TSV output
 *  4. Load TSV output into PostgreSQL
 */
public class MongoDBPipeline {

    private static final String INPUT_DIR = "/input/logs";
    private static final String PIPELINE = "MongoDB";
    private static final String HADOOP_HOME = "/home/priyanshu-tiwari/hadoop";
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String MONGO_DB = "etl_logs";
    private static final String JS_BASE = "src/main/resources/mongo/";

    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] " +
            "\"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)"
    );

    private final String queryName;
    private final int batchSize;
    private final int batchId;

    public MongoDBPipeline(String queryName, int batchSize, int batchId) {
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

        // Step 1: Load raw HDFS logs → MongoDB raw_logs collection
        int[] counts = loadAndCleanToMongo();
        int totalRecords = counts[0];
        int validRecords = counts[1];

        // Step 2: Run selected query aggregation scripts
        if (queryName.equals("Query1") || queryName.equals("All")) runMongosh(
            JS_BASE + "query1.js"
        );
        if (queryName.equals("Query2") || queryName.equals("All")) runMongosh(
            JS_BASE + "query2.js"
        );
        if (queryName.equals("Query3") || queryName.equals("All")) runMongosh(
            JS_BASE + "query3.js"
        );

        int totalBatches =
            totalRecords == 0
                ? 0
                : (int) Math.ceil((double) totalRecords / batchSize);
        double avgBatch =
            totalBatches == 0 ? 0.0 : (double) totalRecords / totalBatches;

        ExecutionStats s = new ExecutionStats();
        s.totalRecords = totalRecords;
        s.malformedRecords = totalRecords - validRecords;
        s.totalBatches = totalBatches;
        s.avgBatchSize = avgBatch;
        s.runtime = System.currentTimeMillis() - start;
        return s;
    }

    public void loadResultsToPostgres(int runId) throws Exception {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        try (
            PostgresWriter w = new PostgresWriter();
            MongoClient mc = MongoClients.create(MONGO_URI)
        ) {
            MongoDatabase db = mc.getDatabase(MONGO_DB);

            if (
                queryName.equals("Query1") || queryName.equals("All")
            ) loadQuery1FromMongo(db, w, runId, ts);
            if (
                queryName.equals("Query2") || queryName.equals("All")
            ) loadQuery2FromMongo(db, w, runId, ts);
            if (
                queryName.equals("Query3") || queryName.equals("All")
            ) loadQuery3FromMongo(db, w, runId, ts);
        }
    }

    // ── Step 1: Load + Clean ─────────────────────────────────────────────────
    private int[] loadAndCleanToMongo() throws Exception {
        int total = 0,
            valid = 0;

        // Resolve the correct HDFS path based on batchId
        String hdfsInput =
            batchId == 1
                ? INPUT_DIR + "/NASA_access_log_Jul95"
                : batchId == 2
                    ? INPUT_DIR + "/NASA_access_log_Aug95"
                    : INPUT_DIR + "/*"; // combined

        try (MongoClient mc = MongoClients.create(MONGO_URI)) {
            MongoDatabase db = mc.getDatabase(MONGO_DB);

            // Drop and recreate collections
            db.getCollection("raw_logs").drop();
            db.getCollection("valid_logs").drop();

            MongoCollection<Document> rawColl = db.getCollection("raw_logs");
            MongoCollection<Document> validColl = db.getCollection(
                "valid_logs"
            );

            // Stream HDFS logs
            ProcessBuilder pb = new ProcessBuilder(
                "bash",
                "-c",
                "hdfs dfs -cat " + hdfsInput
            );
            pb.environment().put("HADOOP_HOME", HADOOP_HOME);
            pb
                .environment()
                .put(
                    "PATH",
                    pb.environment().get("PATH") + ":" + HADOOP_HOME + "/bin"
                );
            pb.redirectErrorStream(false);
            Process proc = pb.start();

            // Drain stderr in a separate thread so it doesn't block stdout
            StringBuilder stderrBuf = new StringBuilder();
            Thread stderrThread = new Thread(() -> {
                try (
                    BufferedReader er = new BufferedReader(
                        new InputStreamReader(proc.getErrorStream())
                    )
                ) {
                    String l;
                    while ((l = er.readLine()) != null) stderrBuf
                        .append(l)
                        .append("\n");
                } catch (IOException ignored) {}
            });
            stderrThread.start();

            List<Document> rawBatch = new ArrayList<>(1000);
            List<Document> validBatch = new ArrayList<>(1000);

            try (
                BufferedReader r = new BufferedReader(
                    new InputStreamReader(proc.getInputStream())
                )
            ) {
                String line;
                while ((line = r.readLine()) != null) {
                    total++;
                    // For combined runs (batchId==0) detect batch from log date: Aug->2, else->1
                    int effectiveBatchId = (batchId != 0)
                        ? batchId
                        : (line.contains("/Aug/") ? 2 : 1);
                    rawBatch.add(
                        new Document("line", line).append(
                            "batch_id",
                            effectiveBatchId
                        )
                    );

                    Matcher m = LOG_PATTERN.matcher(line);
                    if (m.matches()) {
                        valid++;
                        String bytesText = m.group(8);
                        long bytes = bytesText.matches("\\d+")
                            ? Long.parseLong(bytesText)
                            : 0L;
                        validBatch.add(
                            new Document()
                                .append("batch_id", effectiveBatchId)
                                .append("host", m.group(1))
                                .append("log_date", m.group(2))
                                .append(
                                    "log_hour",
                                    Integer.parseInt(m.group(3))
                                )
                                .append("method", m.group(4))
                                .append("resource_path", m.group(5))
                                .append("protocol", m.group(6))
                                .append(
                                    "status_code",
                                    Integer.parseInt(m.group(7))
                                )
                                .append("bytes", bytes)
                        );
                    }

                    if (rawBatch.size() >= 1000) {
                        rawColl.insertMany(rawBatch);
                        rawBatch.clear();
                        validColl.insertMany(validBatch);
                        validBatch.clear();
                    }
                }
            }
            if (!rawBatch.isEmpty()) rawColl.insertMany(rawBatch);
            if (!validBatch.isEmpty()) validColl.insertMany(validBatch);

            stderrThread.join();
            int exitCode = proc.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException(
                    "hdfs dfs -cat failed (exit=" +
                        exitCode +
                        "):\n" +
                        stderrBuf
                );
            }
            System.out.println(
                "  → Loaded " +
                    total +
                    " raw records, " +
                    valid +
                    " valid into MongoDB."
            );
        }
        return new int[] { total, valid };
    }

    // ── Run a mongosh script ─────────────────────────────────────────────────
    private void runMongosh(String jsFile) throws Exception {
        runCommand("mongosh " + MONGO_DB + " --file " + jsFile + " --quiet");
    }

    // ── Load results from MongoDB → PostgreSQL ───────────────────────────────
    private void loadQuery1FromMongo(
        MongoDatabase db,
        PostgresWriter w,
        int runId,
        Timestamp ts
    ) throws Exception {
        for (Document doc : db.getCollection("query1_results").find()) {
            w.insertQuery1(
                runId,
                PIPELINE,
                doc.getInteger("batch_id"),
                ts,
                doc.getString("log_date"),
                doc.getInteger("status_code"),
                toLong(doc, "request_count"),
                toLong(doc, "total_bytes")
            );
        }
    }

    private void loadQuery2FromMongo(
        MongoDatabase db,
        PostgresWriter w,
        int runId,
        Timestamp ts
    ) throws Exception {
        for (Document doc : db.getCollection("query2_results").find()) {
            w.insertQuery2(
                runId,
                PIPELINE,
                doc.getInteger("batch_id"),
                ts,
                doc.getString("resource_path"),
                toLong(doc, "request_count"),
                toLong(doc, "total_bytes"),
                toLong(doc, "distinct_host_count")
            );
        }
    }

    private void loadQuery3FromMongo(
        MongoDatabase db,
        PostgresWriter w,
        int runId,
        Timestamp ts
    ) throws Exception {
        for (Document doc : db.getCollection("query3_results").find()) {
            w.insertQuery3(
                runId,
                PIPELINE,
                doc.getInteger("batch_id"),
                ts,
                doc.getString("log_date"),
                doc.getInteger("log_hour"),
                toLong(doc, "error_request_count"),
                toLong(doc, "total_request_count"),
                doc.getDouble("error_rate"),
                toLong(doc, "distinct_error_hosts")
            );
        }
    }

    private long toLong(Document doc, String field) {
        Object v = doc.get(field);
        if (v instanceof Integer i) return i.longValue();
        if (v instanceof Long l) return l;
        if (v instanceof Double d) return d.longValue();
        return 0L;
    }

    private void runCommand(String command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
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
            "MongoDB command failed: " + command
        );
    }
}
