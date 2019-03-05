package com.sk.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Weather implements WritableComparable<Weather> {
    private Integer year;
    private Integer month;
    private Integer day;
    private Integer temp;

    @Override
    public int compareTo(Weather weather) {
        int yearResult = Integer.compare(this.year, weather.getYear());
        if (yearResult == 0){
            int monthResult = Integer.compare(this.month, weather.getMonth());
            if (monthResult == 0){
                return Integer .compare(this.day, weather.getDay());
            }
            return monthResult;
        }
        return yearResult;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temp = in.readInt();
    }


    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Integer getTemp() {
        return temp;
    }

    public void setTemp(Integer temp) {
        this.temp = temp;
    }
}
