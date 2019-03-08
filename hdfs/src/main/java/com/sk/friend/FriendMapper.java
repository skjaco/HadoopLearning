package com.sk.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text mapKey = new Text();
    IntWritable mapValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = StringUtils.split(value.toString(), ' ');
        for (int i = 1; i < strs.length; i++) {
            mapKey.set(getFriend(strs[0], strs[i]));
            mapValue.set(0);
            context.write(mapKey, mapValue);
            for (int j = i + 1; j < strs.length; j++) {
                mapKey.set(getFriend(strs[i],strs[j]));
                mapValue.set(1);
                context.write(mapKey, mapValue);
            }
        }

    }

    public static String getFriend(String s1, String s2) {
        if (s1.compareTo(s2) < 0) {
            return s1 + ":" + s2;
        }
        return s2 + ":" + s1;
    }
}
