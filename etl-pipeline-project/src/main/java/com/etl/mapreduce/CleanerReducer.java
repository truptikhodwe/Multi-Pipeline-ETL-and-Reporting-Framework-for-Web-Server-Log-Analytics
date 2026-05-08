package com.etl.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Identity reducer – passes every value through unchanged, one line per record. */
public class CleanerReducer extends Reducer<Text, Text, Text, NullWritable> {

    private final NullWritable nullVal = NullWritable.get();
    private final Text outKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        for (Text val : values) {
            outKey.set(val);
            context.write(outKey, nullVal);
        }
    }
}
