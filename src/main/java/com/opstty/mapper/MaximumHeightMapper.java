package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaximumHeightMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text treeKind = new Text();
    private DoubleWritable height = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (columns.length >= 7) {
            String kind = columns[2];
            String heightStr = columns[6];
            if (!heightStr.isEmpty()) {
                try {
                    double treeHeight = Double.parseDouble(heightStr);
                    treeKind.set(kind);
                    height.set(treeHeight);
                    context.write(treeKind, height);
                } catch (NumberFormatException e) {
                    // Skip the record with non-numeric height
                }
            }
        }
    }
}
