package com.stratio.deep.benchmark.cassandra.hadoop.group.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;

public class CassadraGroupReduce
        extends
        Reducer<ContributorWritable, NullWritable, ContributorWritable, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(ContributorWritable key,
            Iterable<NullWritable> values, Context context) throws IOException,
            InterruptedException {
        int i = 0;
        for (@SuppressWarnings("unused")
        NullWritable value : values) {
            i++;
        }
        context.write(key, new IntWritable(i));
    }

}
