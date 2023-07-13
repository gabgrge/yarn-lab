package com.opstty.job;

import com.opstty.mapper.MaxTreeMapper;
import com.opstty.mapper.TreeCountMapper;
import com.opstty.reducer.MaxTreeReducer;
import com.opstty.reducer.TreeCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TreeCountJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tree Count");

        job.setJarByClass(TreeCountJob.class);
        job.setMapperClass(TreeCountMapper.class);
        job.setCombinerClass(TreeCountReducer.class);
        job.setReducerClass(TreeCountReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Find Max Trees");

            job2.setJarByClass(TreeCountJob.class);
            job2.setMapperClass(MaxTreeMapper.class);
            job2.setReducerClass(MaxTreeReducer.class);

            job2.setMapOutputKeyClass(NullWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
