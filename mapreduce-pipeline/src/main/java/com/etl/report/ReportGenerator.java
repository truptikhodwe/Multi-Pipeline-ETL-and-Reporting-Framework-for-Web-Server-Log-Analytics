package com.etl.report;

import com.etl.model.BatchInfo;

import java.util.List;

public class ReportGenerator {

    public static void generate(long runtime,
            long totalRecords,
            long malformedRecords,
            int totalBatches,
            double avgBatchSize,
            List<BatchInfo> batches) {

        System.out.println("\n===== FINAL SUMMARY =====");
        System.out.println("Pipeline: Hadoop MapReduce");
        System.out.println("Runtime: " + runtime + " ms");
        System.out.println("Total Records: " + totalRecords);
        System.out.println("Malformed Records: " + malformedRecords);
        System.out.println("Total Batches: " + totalBatches);
        System.out.println("Average Batch Size: " + avgBatchSize);

        System.out.println("\n===== BATCH SAMPLE =====");
        int sample = Math.min(5, batches.size());
        for (int i = 0; i < sample; i++) {
            BatchInfo b = batches.get(i);
            System.out.println(
                    "Batch " + b.batchId +
                            " | size=" + b.batchSize +
                            " | records " + b.startRecord + " to " + b.endRecord);
        }

        if (batches.size() > 5) {
            System.out.println("...");
            for (int i = Math.max(5, batches.size() - 3); i < batches.size(); i++) {
                BatchInfo b = batches.get(i);
                System.out.println(
                        "Batch " + b.batchId +
                                " | size=" + b.batchSize +
                                " | records " + b.startRecord + " to " + b.endRecord);
            }
        }
    }
}