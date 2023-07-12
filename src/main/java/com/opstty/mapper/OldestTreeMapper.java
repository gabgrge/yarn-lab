package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, NullWritable, OldestTreeWritable> {
    private static final int YEAR_INDEX = 5;
    private static final int DISTRICT_INDEX = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("GEOPOINT")) {
            return; // Ignore the header row
        }

        String[] columns = value.toString().split(";");

        if (columns.length >= 6) {
            String yearStr = columns[YEAR_INDEX].trim();
            String districtStr = columns[DISTRICT_INDEX].trim();

            if (!yearStr.isEmpty() && !districtStr.isEmpty()) {
                int year = Integer.parseInt(yearStr);
                int district = Integer.parseInt(districtStr);

                context.write(NullWritable.get(), new OldestTreeWritable(year, district));
            }
        }
    }
}
