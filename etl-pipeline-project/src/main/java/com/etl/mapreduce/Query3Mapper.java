package com.etl.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Query3Mapper – reads valid_logs TSV, emits:
 *   key   = batch_id\tlog_date\tlog_hour
 *   value = status_code\thost
 *
 * The reducer will count totals, errors, and distinct error hosts.
 */
public class Query3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] p = value.toString().split("\t", -1);
        if (p.length < 9) return;

        outKey.set(p[0] + "\t" + p[2] + "\t" + p[3]);   // batch_id\tdate\thour
        outVal.set(p[7] + "\t" + p[1]);                  // status_code\thost
        context.write(outKey, outVal);
    }
}
