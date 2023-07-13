package com.opstty.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TreeCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable district = new IntWritable();
    private final IntWritable treeCount = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (columns.length > 1 && columns[1].matches("\\d+")) {
            district.set(Integer.parseInt(columns[1]));
            context.write(district, treeCount);
        }
    }
}
