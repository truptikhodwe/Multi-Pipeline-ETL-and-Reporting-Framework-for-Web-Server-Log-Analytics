package com.etl.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Query3Reducer – per (batch_id, date, hour) computes:
 *   total_request_count, error_request_count, error_rate, distinct_error_hosts.
 * Output: batch_id\tdate\thour\terror_count\ttotal_count\terror_rate\tdistinct_error_hosts
 */
public class Query3Reducer extends Reducer<Text, Text, Text, NullWritable> {

    private final NullWritable nullVal = NullWritable.get();
    private final Text outKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        long total = 0,
            errors = 0;
        Set<String> errorHosts = new HashSet<>();

        for (Text v : values) {
            String[] p = v.toString().split("\t", -1);
            int status = Integer.parseInt(p[0]);
            total++;
            if (status >= 400 && status <= 599) {
                errors++;
                if (p.length > 1) errorHosts.add(p[1]);
            }
        }

        double errorRate = total > 0 ? (double) errors / total : 0.0;
        outKey.set(
            key +
                "\t" +
                errors +
                "\t" +
                total +
                "\t" +
                errorRate +
                "\t" +
                errorHosts.size()
        );
        context.write(outKey, nullVal);
    }
}
