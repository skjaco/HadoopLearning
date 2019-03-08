package com.sk.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable reduceValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        int sum = 0;
        for (IntWritable v : values){
            if (v.get() == 0){
                flag = 1;
            }
            sum += v.get();
        }
        if (flag == 0){
            reduceValue.set(sum);
            context.write(key, reduceValue);
        }
    }
}
