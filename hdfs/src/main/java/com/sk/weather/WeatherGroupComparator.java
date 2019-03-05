package com.sk.weather;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherGroupComparator extends WritableComparator {
    public WeatherGroupComparator(){
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather w1 = (Weather)a;
        Weather w2 = (Weather)b;
        int r1 = Integer.compare(w1.getYear(), w2.getYear());
        if (r1 == 0){
            return Integer.compare(w1.getMonth(), w2.getMonth());
        }
        return r1;
    }
}
