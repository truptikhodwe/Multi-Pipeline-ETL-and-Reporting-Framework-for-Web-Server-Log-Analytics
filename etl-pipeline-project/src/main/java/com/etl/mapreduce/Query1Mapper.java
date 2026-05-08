package com.etl.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Query1Mapper – reads valid_logs TSV, emits:
 *   key = batch_id\tlog_date\tstatus_code
 *   value = 1\tbytes
 *
 * Valid-logs columns (0-indexed):
 *   0:batch_id 1:host 2:log_date 3:log_hour 4:method 5:resource_path
 *   6:protocol 7:status_code 8:bytes
 */
public class Query1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] p = value.toString().split("\t", -1);
        if (p.length < 9) return;

        // key: batch_id\tdate\tstatus
        outKey.set(p[0] + "\t" + p[2] + "\t" + p[7]);
        // value: count=1, bytes
        outVal.set("1\t" + p[8]);
        context.write(outKey, outVal);
    }
}
