package com.etl.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Identity reducer – passes every value through unchanged. */
public class CleanerReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        for (Text val : values) {
            // Emit value as key with null value so the output file has no leading tab
            outKey.set(val);
            context.write(outKey, null);
        }
    }
}
