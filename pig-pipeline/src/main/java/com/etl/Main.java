package com.etl;

import com.etl.controller.PigPipeline;
import com.etl.db.PostgresWriter;
import com.etl.model.ExecutionStats;

public class Main {

    public static void main(String[] args) throws Exception {
        PigPipeline pipeline = new PigPipeline();

        ExecutionStats stats = pipeline.run();

        pipeline.copyOutput();

        try (PostgresWriter writer = new PostgresWriter()) {
            writer.clearResults();

            int runId = writer.insertRun(
                    pipeline.getPipelineName(),
                    stats.runtime,
                    pipeline.getBatchSize(),
                    stats.avgBatchSize,
                    stats.totalRecords,
                    stats.malformedRecords
            );

            pipeline.loadResultsToPostgres(runId);

            System.out.println("Run ID: " + runId);
        }
    }
}