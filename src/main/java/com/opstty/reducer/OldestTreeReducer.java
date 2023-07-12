package com.opstty.reducer;

import com.opstty.mapper.OldestTreeWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<NullWritable, OldestTreeWritable, Text, NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<OldestTreeWritable> values, Context context) throws IOException, InterruptedException {
        int oldestYear = Integer.MAX_VALUE;
        int districtWithOldestTree = -1;

        for (OldestTreeWritable value : values) {
            int year = value.getYear();
            int district = value.getDistrict();

            if (year < oldestYear) {
                oldestYear = year;
                districtWithOldestTree = district;
            }
        }

        if (districtWithOldestTree != -1) {
            Text outputKey = new Text(String.valueOf(districtWithOldestTree));
            context.write(outputKey, NullWritable.get());
        }
    }
}

