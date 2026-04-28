package com.etl.mapreduce;

import com.etl.parser.LogParser;
import com.etl.model.LogRecord;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1Job {

        public static enum COUNTERS {
                MALFORMED_RECORDS
        };

        public static class Q1Mapper
                        extends Mapper<LongWritable, Text, Text, Text> {

                public void map(
                                LongWritable key,
                                Text value,
                                Context context) throws IOException, InterruptedException {

                        String line = value.toString();

                        LogRecord r = LogParser.parse(line);

                        if (r == null) {
                                context.getCounter(COUNTERS.MALFORMED_RECORDS).increment(1);
                                return;
                        }

                        String outKey = r.date + "|" + r.status;
                        String outVal = "1," + r.bytes;

                        context.write(new Text(outKey), new Text(outVal));

                }
        }

        public static class Q1Reducer
                        extends Reducer<Text, Text, Text, Text> {

                public void reduce(
                                Text key,
                                Iterable<Text> values,
                                Context context) throws IOException, InterruptedException {

                        long count = 0;
                        long totalBytes = 0;

                        for (Text v : values) {

                                String[] parts = v.toString().split(",");

                                count += Long.parseLong(parts[0]);
                                totalBytes += Long.parseLong(parts[1]);

                        }

                        context.write(key, new Text(count + "," + totalBytes));

                }
        }

        public static long run(String in, String out) throws Exception {

                Configuration conf = new Configuration();

                conf.set("fs.defaultFS", "hdfs://localhost:9000");

                conf.set("mapreduce.framework.name", "yarn");

                Job job = Job.getInstance(conf, "Query1");

                job.setJarByClass(Query1Job.class);

                job.setMapperClass(Q1Mapper.class);

                job.setReducerClass(Q1Reducer.class);

                job.setOutputKeyClass(Text.class);

                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(in));

                FileOutputFormat.setOutputPath(job, new Path(out));

                job.waitForCompletion(true);

                long malformed = job.getCounters()
                                .findCounter(COUNTERS.MALFORMED_RECORDS)
                                .getValue();

                System.out.println("\nMalformed Records: " + malformed);

                return malformed;

        }

}