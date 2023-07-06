package com.opstty.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DistrictsContainingTreesReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
