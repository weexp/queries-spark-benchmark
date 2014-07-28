package com.stratio.deep.benchmark.common.hadoop.counter.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

public class CounterReducer extends
        Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values,
            Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(Iterables.size(values)),
                NullWritable.get());
    }
}
