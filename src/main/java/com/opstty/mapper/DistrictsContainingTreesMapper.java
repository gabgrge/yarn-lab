package com.opstty.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistrictsContainingTreesMapper extends Mapper<Object, Text, IntWritable, NullWritable> {

    private IntWritable district = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("GEOPOINT")) {  // Check if the line starts with the header row
            return;  // Skip the header row
        }

        String[] columns = line.split(";");
        int arrondissement;

        try {
            arrondissement = Integer.parseInt(columns[1]);  // Assuming the arrondissement column is at index 1
        } catch (NumberFormatException e) {
            // Handle invalid district values, e.g., if it's not a valid integer
            return;
        }

        district.set(arrondissement);
        context.write(district, NullWritable.get());
    }
}
