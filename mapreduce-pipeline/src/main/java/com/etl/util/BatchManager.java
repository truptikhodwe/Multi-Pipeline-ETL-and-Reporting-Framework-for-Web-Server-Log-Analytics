package com.etl.util;

import com.etl.model.BatchInfo;

import java.io.*;
import java.util.*;

public class BatchManager {

    private long totalRecords = 0;

    public long getTotalRecords() {
        return totalRecords;
    }

    public List<BatchInfo> createBatches(
            String hdfsInputPath,
            int batchSize) throws Exception {

        List<BatchInfo> batches = new ArrayList<>();

        Process process = new ProcessBuilder(
                "bash",
                "-c",
                "hdfs dfs -cat " + hdfsInputPath + "/*").redirectErrorStream(true)
                .start();

        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        process.getInputStream()));

        String line;

        int currentBatch = 0;
        int batchId = 1;
        long startRecord = 1;

        while ((line = br.readLine()) != null) {

            totalRecords++;
            currentBatch++;

            if (currentBatch == 1)
                startRecord = totalRecords;

            if (currentBatch == batchSize) {

                batches.add(
                        new BatchInfo(
                                batchId,
                                currentBatch,
                                startRecord,
                                totalRecords));

                batchId++;
                currentBatch = 0;
            }

        }

        if (currentBatch > 0) {

            batches.add(
                    new BatchInfo(
                            batchId,
                            currentBatch,
                            startRecord,
                            totalRecords));

        }

        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException(
                    "Failed to read HDFS input file for batch creation.");
        }

        return batches;

    }

}