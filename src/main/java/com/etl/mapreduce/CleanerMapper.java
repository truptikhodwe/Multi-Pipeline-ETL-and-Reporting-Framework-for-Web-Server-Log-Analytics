package com.etl.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CleanerMapper – parses raw Apache HTTP log lines.
 * Batch ID is derived from the input filename: Jul→1, Aug→2, else→1.
 * Emits: key=NullWritable, value=batch_id\thost\tdate\thour\tmethod\tpath\tprotocol\tstatus\tbytes
 */
public class CleanerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+) - - \\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] " +
        "\"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)"
    );

    private int batchId;
    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) {
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        batchId = filename.contains("Aug") ? 2 : 1;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Matcher m = LOG_PATTERN.matcher(value.toString());
        if (!m.matches()) return;

        String bytesText = m.group(8);
        long bytes = bytesText.matches("\\d+") ? Long.parseLong(bytesText) : 0L;

        // Key: batch_id (for grouping by batch); Value: all fields tab-separated
        outKey.set(String.valueOf(batchId));
        outValue.set(batchId + "\t" + m.group(1) + "\t" + m.group(2) + "\t" +
                     m.group(3) + "\t" + m.group(4) + "\t" + m.group(5) + "\t" +
                     m.group(6) + "\t" + m.group(7) + "\t" + bytes);
        context.write(outKey, outValue);
    }
}
