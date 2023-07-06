package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaximumHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable maxHeight = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double max = Double.MIN_VALUE;
        for (DoubleWritable value : values) {
            double height = value.get();
            if (height > max) {
                max = height;
            }
        }
        maxHeight.set(max);
        context.write(key, maxHeight);
    }
}
