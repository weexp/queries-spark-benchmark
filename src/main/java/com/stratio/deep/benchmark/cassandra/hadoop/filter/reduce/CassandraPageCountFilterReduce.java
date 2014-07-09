package com.stratio.deep.benchmark.cassandra.hadoop.filter.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CassandraPageCountFilterReduce extends
        Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values,
            Context context) throws IOException, InterruptedException {
        int i = 0;
        for (@SuppressWarnings("unused")
        NullWritable value : values) {
            i++;
        }
        context.write(new IntWritable(i), NullWritable.get());
    }
}
