package com.sk.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * hadoop开发三步
 * 1.客户端
 * 2.map
 * 3.reduce
 */
public class WeatherTest1 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);
        job.setJarByClass(WeatherTest1.class);
        // ------job配置内容------
        // map 配置
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(WeatherPartitioner.class);
        job.setSortComparatorClass(WeatherComparator.class);

        // reduce 配置
        job.setGroupingComparatorClass(WeatherGroupComparator.class);
        job.setReducerClass(WeatherReduce.class);

        // 配置杂项
        Path inputPath = new Path("/test/weather/input/weather.txt");
        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path("/test/weather/output");
        if (outputPath.getFileSystem(conf).exists(outputPath)) {
            outputPath.getFileSystem(conf).delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setNumReduceTasks(2);
        // ---------------------
        job.waitForCompletion(true);
    }
}
