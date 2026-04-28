package com.etl.model;

public class BatchInfo {
    public int batchId;
    public int batchSize;
    public long startRecord;
    public long endRecord;

    public BatchInfo(int batchId, int batchSize, long startRecord, long endRecord) {
        this.batchId = batchId;
        this.batchSize = batchSize;
        this.startRecord = startRecord;
        this.endRecord = endRecord;
    }
}