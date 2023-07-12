package com.opstty.mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TreeHeightMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable height = new DoubleWritable();
    private Text treeInfo = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("GEOPOINT")) {
            return; // Ignore the header row
        }

        String[] columns = value.toString().split(";");
        if (columns.length >= 8) {
            String heightStr = columns[6];
            if (!heightStr.isEmpty()) {
                double treeHeight = Double.parseDouble(heightStr);
                height.set(treeHeight);
                treeInfo.set(value);
                context.write(height, treeInfo);
            }
        }
    }
}

