package com.etl.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Query2Reducer – aggregates per (batch_id, path):
 *   request_count, total_bytes, distinct_host_count.
 * Output: batch_id\tpath\trequest_count\ttotal_bytes\tdistinct_host_count
 */
public class Query2Reducer extends Reducer<Text, Text, Text, NullWritable> {

    private final NullWritable nullVal = NullWritable.get();
    private final Text outKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        long count = 0,
            bytes = 0;
        Set<String> hosts = new HashSet<>();

        for (Text v : values) {
            String[] p = v.toString().split("\t", -1);
            count += Long.parseLong(p[0]);
            bytes += Long.parseLong(p[1]);
            if (p.length > 2) hosts.add(p[2]);
        }
        outKey.set(key + "\t" + count + "\t" + bytes + "\t" + hosts.size());
        context.write(outKey, nullVal);
    }
}
