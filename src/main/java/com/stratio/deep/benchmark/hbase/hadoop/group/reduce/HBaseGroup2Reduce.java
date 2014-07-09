package com.stratio.deep.benchmark.hbase.hadoop.group.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.stratio.deep.benchmark.model.ContributorWritable;

public class HBaseGroup2Reduce
        extends
        Reducer<ContributorWritable, IntWritable, ContributorWritable, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(ContributorWritable key,
            Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        int i = 0;
        for (IntWritable value : values) {
            i += value.get();
        }
        context.write(key, new IntWritable(i));
    }

}
