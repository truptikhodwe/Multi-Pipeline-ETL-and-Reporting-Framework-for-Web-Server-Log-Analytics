package com.etl.controller;
import com.etl.util.LoggerUtil;

import com.etl.parser.LogParser;
import com.etl.model.LogRecord;
import com.etl.util.BatchReader;

import com.mongodb.client.*;

import com.etl.model.ExecutionStats;

import org.bson.Document;

import java.io.*;
import java.util.*;

public class MongoPipeline {

    private MongoCollection<Document> collection;
    private List<int[]> batchInfo = new ArrayList<>();

    public MongoPipeline() {
        MongoClient client = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase db = client.getDatabase("etl_db");
        collection = db.getCollection("logs");

        // Clean previous data
        collection.drop();
    }

    public ExecutionStats execute(String filePath, int batchSize) throws Exception {

    BufferedReader br = new BufferedReader(new FileReader(filePath));

    int batchId = 1;
    int malformed = 0;
    int total = 0;
    int totalBatches = 0;

    LoggerUtil.clear();

    while (true) {
        List<String> lines = BatchReader.readBatch(br, batchSize);
        if (lines.isEmpty()) break;

        totalBatches++;

        List<Document> docs = new ArrayList<>();
        // List<int[]> batchInfo = new ArrayList<>();
        for (String line : lines) {
            total++;

            LogRecord r = LogParser.parse(line);

            if (r == null) {
                malformed++;
                continue;
            }

            docs.add(new Document("host", r.host)
                    .append("date", r.date)
                    .append("hour", r.hour)
                    .append("method", r.method)
                    .append("path", r.path)
                    .append("protocol", r.protocol)
                    .append("status", r.status)
                    .append("bytes", r.bytes)
                    .append("batch_id", batchId));
        }

        if (!docs.isEmpty()) {
            collection.insertMany(docs);
        }

        // com.etl.db.PostgresWriter writer = new com.etl.db.PostgresWriter();
        // writer.insertBatch(runId, batchId, lines.size());
        batchInfo.add(new int[]{batchId, lines.size()});


        String msg = "Processed batch " + batchId + " size=" + lines.size();
        System.out.println(msg);
        LoggerUtil.log(msg);

        batchId++;
    }

    br.close();

    ExecutionStats stats = new ExecutionStats();
    stats.totalRecords = total;
    stats.malformedRecords = malformed;
    stats.totalBatches = totalBatches;
    stats.avgBatchSize = (double) total / totalBatches;

    return stats;
}

    public void runQueriesToPostgres(int runId) throws Exception {

        com.etl.db.PostgresWriter writer = new com.etl.db.PostgresWriter();
    
        // Query 1
        for (Document doc : collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id",
                        new Document("date", "$date").append("status", "$status"))
                        .append("request_count", new Document("$sum", 1))
                        .append("total_bytes", new Document("$sum", "$bytes")))
        ))) {
    
            Document id = (Document) doc.get("_id");
    
            writer.insertQuery1(
                    runId,
                    id.getString("date"),
                    id.getInteger("status"),
                    doc.getInteger("request_count"),
                    doc.getLong("total_bytes")
            );
        }
    
        // Query 2
        for (Document doc : collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$path")
                        .append("request_count", new Document("$sum", 1))
                        .append("total_bytes", new Document("$sum", "$bytes"))
                        .append("hosts", new Document("$addToSet", "$host"))),
                new Document("$addFields",
                        new Document("distinct_hosts", new Document("$size", "$hosts"))),
                new Document("$sort", new Document("request_count", -1)),
                new Document("$limit", 20)
        ))) {
    
            writer.insertQuery2(
                    runId,
                    doc.getString("_id"),
                    doc.getInteger("request_count"),
                    doc.getLong("total_bytes"),
                    doc.getInteger("distinct_hosts")
            );
        }
    
        // Query 3
        // for (Document doc : collection.aggregate(Arrays.asList(
        //         new Document("$match",
        //                 new Document("status",
        //                         new Document("$gte", 400).append("$lte", 599))),
        //         new Document("$group", new Document("_id",
        //                 new Document("date", "$date").append("hour", "$hour"))
        //                 .append("error_count", new Document("$sum", 1))
        //                 .append("hosts", new Document("$addToSet", "$host"))),
        //         new Document("$addFields",
        //                 new Document("distinct_hosts", new Document("$size", "$hosts")))
        // ))) {
    
        //     Document id = (Document) doc.get("_id");
    
        //     writer.insertQuery3(
        //             runId,
        //             id.getString("date"),
        //             id.getInteger("hour"),
        //             doc.getInteger("error_count"),
        //             doc.getInteger("distinct_hosts")
        //     );
        // }
        // Query 3 (FULL FIXED VERSION)
            for (Document doc : collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id",
                        new Document("date", "$date").append("hour", "$hour"))
                        .append("total_requests", new Document("$sum", 1))
                        .append("error_count",
                                new Document("$sum",
                                        new Document("$cond", Arrays.asList(
                                                new Document("$and", Arrays.asList(
                                                        new Document("$gte", Arrays.asList("$status", 400)),
                                                        new Document("$lte", Arrays.asList("$status", 599))
                                                )),
                                                1,
                                                0
                                        ))
                                ))
                        .append("error_hosts",
                                new Document("$addToSet",
                                        new Document("$cond", Arrays.asList(
                                                new Document("$and", Arrays.asList(
                                                        new Document("$gte", Arrays.asList("$status", 400)),
                                                        new Document("$lte", Arrays.asList("$status", 599))
                                                )),
                                                "$host",
                                                null
                                        ))
                                ))
                ),

                new Document("$addFields",
                        new Document("distinct_error_hosts",
                                new Document("$size", "$error_hosts"))
                                .append("error_rate",
                                        new Document("$divide", Arrays.asList("$error_count", "$total_requests")))
                )
            ))) {

            Document id = (Document) doc.get("_id");

            writer.insertQuery3(
                    runId,
                    id.getString("date"),
                    id.getInteger("hour"),
                    doc.getInteger("error_count"),
                    doc.getInteger("total_requests"),
                    doc.getDouble("error_rate"),
                    doc.getInteger("distinct_error_hosts")
            );
            }
    }

    public List<int[]> getBatchInfo() {
        return batchInfo;
    }
}