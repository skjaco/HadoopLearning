package com.sk.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCount.class);

        // Specify various job-specific parameters
        job.setJobName("com.sk.mapreduce.WordCount");

//      job.setInputPath(new Path("in"));
//      job.setOutputPath(new Path("out"));

        FileInputFormat.addInputPath(job, new Path("/dir/test.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/data/word"));
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
