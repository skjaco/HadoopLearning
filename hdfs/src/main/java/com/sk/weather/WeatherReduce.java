package com.sk.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReduce extends Reducer<Weather, IntWritable, Text, IntWritable> {

    Text reduceKey = new Text();
    IntWritable reduceValue = new IntWritable();

    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        int day = 0;
        for (IntWritable v : values) {
            if (flag == 0) {
                reduceKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + ":" + key.getTemp());
                reduceValue.set(key.getTemp());
                flag++;
                day = key.getDay();
                context.write(reduceKey, reduceValue);
            }
            if (flag != 0 && day != 0) {
                reduceKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + ":" + key.getTemp());
                reduceValue.set(key.getTemp());
                day = key.getDay();
                context.write(reduceKey, reduceValue);
                break;
            }
        }
        super.reduce(key, values, context);
    }
}
