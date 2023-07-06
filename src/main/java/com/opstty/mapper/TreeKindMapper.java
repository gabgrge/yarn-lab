package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeKindMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text kind = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("GEOPOINT")) {
            return; // Ignore the header row
        }

        String[] columns = value.toString().split(";");
        if (columns.length >= 3) {
            String treeKind = columns[2]; // Assuming the kind column is at index 2
            kind.set(treeKind);
            context.write(kind, one);
        }
    }
}
