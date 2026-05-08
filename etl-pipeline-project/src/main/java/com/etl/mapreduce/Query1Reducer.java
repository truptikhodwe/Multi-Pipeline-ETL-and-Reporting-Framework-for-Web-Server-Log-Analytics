package com.etl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Query1Reducer – sums request_count and total_bytes per (batch_id, date, status).
 * Output: batch_id\tlog_date\tstatus_code\trequest_count\ttotal_bytes
 */
public class Query1Reducer extends Reducer<Text, Text, Text, Text> {

    private final Text outKey = new Text("");
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long count = 0, bytes = 0;
        for (Text v : values) {
            String[] p = v.toString().split("\t", -1);
            count += Long.parseLong(p[0]);
            bytes += Long.parseLong(p[1]);
        }
        outVal.set(key + "\t" + count + "\t" + bytes);
        context.write(outKey, outVal);
    }
}
