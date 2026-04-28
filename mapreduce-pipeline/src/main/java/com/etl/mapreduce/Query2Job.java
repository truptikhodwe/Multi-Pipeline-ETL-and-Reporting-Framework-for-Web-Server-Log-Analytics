package com.etl.mapreduce;

import com.etl.parser.LogParser;
import com.etl.model.LogRecord;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query2Job {

    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(
                LongWritable key,
                Text value,
                Context context) throws IOException, InterruptedException {

            LogRecord r = LogParser.parse(
                    value.toString());

            if (r == null)
                return;

            context.write(
                    new Text(r.path),
                    new Text(r.host + "," + r.bytes));

        }
    }

    public static class Reducer2
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(
                Text key,
                Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            long count = 0;
            long bytes = 0;

            HashSet<String> hosts = new HashSet<>();

            for (Text v : values) {

                String val = v.toString();

                int idx = val.lastIndexOf(",");

                if (idx == -1)
                    continue;

                String host = val.substring(0, idx);

                String bytesStr = val.substring(idx + 1);

                try {
                    bytes += Long.parseLong(bytesStr);
                } catch (Exception e) {
                    continue;
                }

                count++;
                hosts.add(host);

            }

            context.write(
                    key,
                    new Text(
                            count +
                                    "," +
                                    bytes +
                                    "," +
                                    hosts.size()));

        }
    }

    public static void run(
            String in,
            String out) throws Exception {

        Configuration conf = new Configuration();

        conf.set(
                "fs.defaultFS",
                "hdfs://localhost:9000");

        conf.set(
                "mapreduce.framework.name",
                "yarn");
        Job job = Job.getInstance(
                conf,
                "Query2");

        job.setJarByClass(
                Query2Job.class);

        job.setMapperClass(
                Mapper2.class);

        job.setReducerClass(
                Reducer2.class);

        job.setOutputKeyClass(
                Text.class);

        job.setOutputValueClass(
                Text.class);

        FileInputFormat.addInputPath(
                job,
                new Path(in));

        FileOutputFormat.setOutputPath(
                job,
                new Path(out));

        job.waitForCompletion(true);

    }

}