// MongoDB ETL – Query 1: Daily Traffic Summary
// Input collection: valid_logs
// Run via: mongosh etl_logs --file query1.js
// Output collection: query1_results (also printed as JSON for parsing)

db = db.getSiblingDB("etl_logs");

db.query1_results.drop();

db.valid_logs.aggregate([
    {
        $group: {
            _id: {
                batch_id:    "$batch_id",
                log_date:    "$log_date",
                status_code: "$status_code"
            },
            request_count: { $sum: 1 },
            total_bytes:   { $sum: "$bytes" }
        }
    },
    {
        $project: {
            _id: 0,
            batch_id:      "$_id.batch_id",
            log_date:      "$_id.log_date",
            status_code:   "$_id.status_code",
            request_count: 1,
            total_bytes:   1
        }
    },
    { $sort: { log_date: 1, status_code: 1 } },
    { $out: "query1_results" }
]);

// Print as tab-separated for Java to parse
db.query1_results.find({}).forEach(function(doc) {
    print(doc.batch_id + "\t" + doc.log_date + "\t" + doc.status_code + "\t" +
          doc.request_count + "\t" + doc.total_bytes);
});
