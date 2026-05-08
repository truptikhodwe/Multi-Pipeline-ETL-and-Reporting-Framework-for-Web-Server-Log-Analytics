package com.etl.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * HadoopRunner – generic Tool that constructs and submits a MapReduce job.
 *
 * Usage (called by MapReducePipeline via hadoop jar):
 *   hadoop jar etl-pipeline-1.0.jar com.etl.mapreduce.HadoopRunner \
 *       com.etl.mapreduce.Query1Mapper com.etl.mapreduce.Query1Reducer \
 *       /input/path /output/path
 */
public class HadoopRunner {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: HadoopRunner <MapperClass> <ReducerClass> <input> <output>");
            System.exit(1);
        }

        String mapperClassName  = args[0];
        String reducerClassName = args[1];
        String inputPath        = args[2];
        String outputPath       = args[3];

        Class<? extends Mapper>  mapperClass  = (Class<? extends Mapper>)  Class.forName(mapperClassName);
        Class<? extends Reducer> reducerClass = (Class<? extends Reducer>) Class.forName(reducerClassName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, mapperClass.getSimpleName() + " → " + reducerClass.getSimpleName());

        job.setJarByClass(HadoopRunner.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
