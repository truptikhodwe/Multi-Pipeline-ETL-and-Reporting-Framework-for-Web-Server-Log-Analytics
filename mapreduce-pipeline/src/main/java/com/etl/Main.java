package com.etl;

import com.etl.db.PostgresWriter;
import com.etl.mapreduce.Query1Job;
import com.etl.mapreduce.Query2Job;
import com.etl.mapreduce.Query3Job;
import com.etl.model.BatchInfo;
import com.etl.report.ReportGenerator;
import com.etl.util.BatchManager;

import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {

        // String hdfsInputPath = "/input/NASA_access_log_Jul95";
        String hdfsInputPath = "/input";
        // String mrInputPath = "hdfs://localhost:9000/input/NASA_access_log_Jul95";
        String mrInputPath = "hdfs://localhost:9000/input";
        String query1Out = "/query1_out";
        String query2Out = "/query2_out";
        String query3Out = "/query3_out";

        int batchSize = 1000;

        BatchManager batchManager = new BatchManager();
        List<BatchInfo> batches = batchManager.createBatches(hdfsInputPath, batchSize);
        long totalRecords = batchManager.getTotalRecords();
        int totalBatches = batches.size();
        double avgBatchSize = totalBatches == 0 ? 0.0 : (double) totalRecords / totalBatches;

        long start = System.currentTimeMillis();

        long malformedRecords = Query1Job.run(mrInputPath, query1Out);
        Query2Job.run(mrInputPath, query2Out);
        Query3Job.run(mrInputPath, query3Out);

        long end = System.currentTimeMillis();
        long runtime = end - start;

        try (PostgresWriter writer = new PostgresWriter()) {
            int runId = writer.insertRunMetrics(
                    "Hadoop MapReduce",
                    runtime,
                    totalRecords,
                    malformedRecords,
                    totalBatches,
                    avgBatchSize);

            writer.insertBatchMetadata(runId, batches);
            writer.loadQuery1Results(runId, query1Out);
            writer.loadQuery2Top20Results(runId, query2Out);
            writer.loadQuery3Results(runId, query3Out);

            ReportGenerator.generate(
                    runtime,
                    totalRecords,
                    malformedRecords,
                    totalBatches,
                    avgBatchSize,
                    batches);
        }
    }
}