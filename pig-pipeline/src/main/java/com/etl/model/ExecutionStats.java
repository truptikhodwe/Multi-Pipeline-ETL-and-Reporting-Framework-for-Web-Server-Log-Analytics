package com.etl.model;

public class ExecutionStats {

    public int totalRecords;
    public int malformedRecords;
    public int totalBatches;
    public double avgBatchSize;
    public long runtime;

    public ExecutionStats() {
        this.totalRecords = 0;
        this.malformedRecords = 0;
        this.totalBatches = 0;
        this.avgBatchSize = 0.0;
    }

    @Override
    public String toString() {
        return "ExecutionStats{" +
                "totalRecords=" + totalRecords +
                ", malformedRecords=" + malformedRecords +
                ", totalBatches=" + totalBatches +
                ", avgBatchSize=" + avgBatchSize +
                ", runtime=" + runtime +
                '}';
    }
}