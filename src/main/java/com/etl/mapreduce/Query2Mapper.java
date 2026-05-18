package com.etl.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Query2Mapper – reads valid_logs TSV, emits:
 *   key   = batch_id\tresource_path
 *   value = 1\tbytes\thost
 */
public class Query2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] p = value.toString().split("\t", -1);
        if (p.length < 9) return;

        outKey.set(p[0] + "\t" + p[5]);          // batch_id\tpath
        outVal.set("1\t" + p[8] + "\t" + p[1]);  // count\tbytes\thost
        context.write(outKey, outVal);
    }
}
