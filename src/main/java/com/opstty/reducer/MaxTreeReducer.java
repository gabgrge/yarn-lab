package com.opstty.reducer;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTreeReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

    private final TreeMap<Integer, Text> treeCounts = new TreeMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        for (Text val : values) {
            String district = val.toString();
            String treeCountStr = conf.get(district);

            if (treeCountStr != null) {
                int treeCount = Integer.parseInt(treeCountStr);

                treeCounts.put(treeCount, new Text(district));
                if (treeCounts.size() > 1) {
                    treeCounts.remove(treeCounts.firstKey());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!treeCounts.isEmpty()) {
            Text resultDistrict = treeCounts.lastEntry().getValue();
            int resultCount = treeCounts.lastKey();

            context.write(resultDistrict, new IntWritable(resultCount));
        }
    }
}
