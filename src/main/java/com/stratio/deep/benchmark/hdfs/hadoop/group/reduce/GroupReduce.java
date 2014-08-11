package com.stratio.deep.benchmark.hdfs.hadoop.group.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupReduce extends Reducer<Text, NullWritable, Text, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,
            Context context) throws IOException, InterruptedException {
        int i = 0;
        for (@SuppressWarnings("unused")
        NullWritable value : values) {
            i++;
        }
        context.write(key, new IntWritable(i));
    }

}
