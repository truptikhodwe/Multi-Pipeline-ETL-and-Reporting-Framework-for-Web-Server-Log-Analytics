package com.etl.mapreduce;

import com.etl.model.LogRecord;
import com.etl.parser.LogParser;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3Job {

    public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            LogRecord r = LogParser.parse(value.toString());
            if (r == null) {
                return;
            }

            String outKey = r.date + "|" + r.hour;
            String outVal = r.status + "," + r.host;

            context.write(new Text(outKey), new Text(outVal));
        }
    }

    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long totalRequests = 0;
            long errorRequests = 0;
            HashSet<String> errorHosts = new HashSet<>();

            for (Text v : values) {
                totalRequests++;

                String[] parts = v.toString().split(",", 2);
                if (parts.length < 2) {
                    continue;
                }

                try {
                    int status = Integer.parseInt(parts[0].trim());
                    String host = parts[1].trim();

                    if (status >= 400 && status <= 599) {
                        errorRequests++;
                        errorHosts.add(host);
                    }
                } catch (Exception e) {
                    // skip bad record
                }
            }

            double errorRate = 0.0;
            if (totalRequests > 0) {
                errorRate = (double) errorRequests / (double) totalRequests;
            }

            String outputValue = totalRequests + "," +
                    errorRequests + "," +
                    errorRate + "," +
                    errorHosts.size();

            context.write(key, new Text(outputValue));
        }
    }

    public static void run(String in, String out) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3Job.class);

        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }
}