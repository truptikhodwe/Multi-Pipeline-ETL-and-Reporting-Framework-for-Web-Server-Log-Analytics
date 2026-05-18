// MongoDB ETL – Query 2: Top 20 Requested Resources
// Input collection: valid_logs
// Run via: mongosh etl_logs --file query2.js

db = db.getSiblingDB("etl_logs");

db.query2_results.drop();

db.valid_logs.aggregate([
    {
        $group: {
            _id: {
                batch_id:      "$batch_id",
                resource_path: "$resource_path"
            },
            request_count:  { $sum: 1 },
            total_bytes:    { $sum: "$bytes" },
            hosts:          { $addToSet: "$host" }
        }
    },
    {
        $project: {
            _id: 0,
            batch_id:           "$_id.batch_id",
            resource_path:      "$_id.resource_path",
            request_count:      1,
            total_bytes:        1,
            distinct_host_count: { $size: "$hosts" }
        }
    },
    { $sort: { request_count: -1 } },
    { $limit: 20 },
    { $out: "query2_results" }
]);

db.query2_results.find({}).forEach(function(doc) {
    print(doc.batch_id + "\t" + doc.resource_path + "\t" + doc.request_count + "\t" +
          doc.total_bytes + "\t" + doc.distinct_host_count);
});
