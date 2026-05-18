// MongoDB ETL – Query 3: Hourly Error Analysis
// Input collection: valid_logs
// Run via: mongosh etl_logs --file query3.js

db = db.getSiblingDB("etl_logs");

db.query3_results.drop();

// Total requests per (batch_id, date, hour)
var totalColl = db.valid_logs.aggregate([
    {
        $group: {
            _id: { batch_id: "$batch_id", log_date: "$log_date", log_hour: "$log_hour" },
            total_count: { $sum: 1 }
        }
    }
]).toArray();

// Error requests per (batch_id, date, hour) – status 400–599
var errorColl = db.valid_logs.aggregate([
    { $match: { status_code: { $gte: 400, $lte: 599 } } },
    {
        $group: {
            _id: { batch_id: "$batch_id", log_date: "$log_date", log_hour: "$log_hour" },
            error_count:          { $sum: 1 },
            distinct_error_hosts: { $addToSet: "$host" }
        }
    }
]).toArray();

// Build error lookup map
var errorMap = {};
errorColl.forEach(function(e) {
    var key = e._id.batch_id + "|" + e._id.log_date + "|" + e._id.log_hour;
    errorMap[key] = { error_count: e.error_count, host_count: e.distinct_error_hosts.length };
});

// Merge and insert
var results = [];
totalColl.forEach(function(t) {
    var key      = t._id.batch_id + "|" + t._id.log_date + "|" + t._id.log_hour;
    var errData  = errorMap[key] || { error_count: 0, host_count: 0 };
    var errRate  = t.total_count > 0 ? errData.error_count / t.total_count : 0.0;

    results.push({
        batch_id:             t._id.batch_id,
        log_date:             t._id.log_date,
        log_hour:             t._id.log_hour,
        error_request_count:  errData.error_count,
        total_request_count:  t.total_count,
        error_rate:           errRate,
        distinct_error_hosts: errData.host_count
    });
});

results.sort(function(a, b) {
    if (a.log_date < b.log_date) return -1;
    if (a.log_date > b.log_date) return  1;
    return a.log_hour - b.log_hour;
});

if (results.length > 0) db.query3_results.insertMany(results);

db.query3_results.find({}).forEach(function(doc) {
    print(doc.batch_id + "\t" + doc.log_date + "\t" + doc.log_hour + "\t" +
          doc.error_request_count + "\t" + doc.total_request_count + "\t" +
          doc.error_rate + "\t" + doc.distinct_error_hosts);
});
