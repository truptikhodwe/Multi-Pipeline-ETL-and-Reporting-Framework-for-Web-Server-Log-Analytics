package com.etl;

import com.etl.controller.MongoPipeline;
import com.etl.db.PostgresWriter;
import com.etl.model.ExecutionStats;
import com.etl.util.LoggerUtil;
import com.etl.report.ReportGenerator;

public class Main {

    public static void main(String[] args) throws Exception {

        String filePath = "data/NASA_access_log_Jul95";
        int batchSize = 1000;

        MongoPipeline pipeline = new MongoPipeline();

        long start = System.currentTimeMillis();

        ExecutionStats stats = pipeline.execute(filePath, batchSize);

        long end = System.currentTimeMillis();
        long runtime = end - start;

        // DB insert
        PostgresWriter writer = new PostgresWriter();

        int runId = writer.insertRun(
                "MongoDB",
                runtime,
                batchSize,
                stats.avgBatchSize,
                stats.totalRecords,
                stats.malformedRecords
        );
        for (int[] b : pipeline.getBatchInfo()) {
            writer.insertBatch(runId, b[0], b[1]);
        }

        // store query results
        pipeline.runQueriesToPostgres(runId);

        String summary =
                "\n===== FINAL SUMMARY =====\n" +
                "Runtime: " + runtime + " ms\n" +
                "Total: " + stats.totalRecords + "\n" +
                "Malformed: " + stats.malformedRecords + "\n" +
                "Batches: " + stats.totalBatches + "\n" +
                "Avg batch: " + stats.avgBatchSize;

        System.out.println(summary);
        LoggerUtil.log(summary);

        ReportGenerator report = new ReportGenerator();
        report.generateReport(runId);
    }
}