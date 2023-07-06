package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text species = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("GEOPOINT")) {  // Check if the line starts with the header row
            return;  // Skip the header row
        }

        String[] columns = value.toString().split(";");
        String speciesName = columns[3]; // Assuming the species column is at index 3

        species.set(speciesName);
        context.write(species, new Text());
    }
}
