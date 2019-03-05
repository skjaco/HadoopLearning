package com.sk.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.SimpleFormatter;

public class WeatherMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {
    Weather mapKey = new Weather();
    IntWritable mapValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1992-01-22 12:22:42  12C
        String[] s1=StringUtils.split(value.toString(), '\t');
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = format.parse(s1[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            mapKey.setYear(calendar.get(Calendar.YEAR));
            mapKey.setMonth(calendar.get(Calendar.MONDAY)+1);
            mapKey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            int temp = Integer.parseInt(s1[1].substring(0, s1[1].length()-1));
            mapKey.setTemp(temp);
            mapValue.set(temp);
            context.write(mapKey, mapValue);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
