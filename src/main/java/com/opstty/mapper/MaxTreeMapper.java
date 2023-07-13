package com.opstty.mapper;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTreeMapper extends Mapper<Object, Text, NullWritable, Text> {

    private Text maxDistrict = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split("\t");
        if (columns.length == 2) {
            maxDistrict.set(columns[0]);
            context.write(NullWritable.get(), maxDistrict);
        }
    }
}
