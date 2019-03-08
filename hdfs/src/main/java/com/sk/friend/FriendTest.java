package com.sk.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendTest.class);

        job.setMapperClass(FriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(FriendReduce.class);

        Path input = new Path("/test/friend/input/friend.txt");
        FileInputFormat.addInputPath(job, input);
        Path output = new Path("/test/friend/output");
        if (output.getFileSystem(conf).exists(output)) {
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);

    }
}
