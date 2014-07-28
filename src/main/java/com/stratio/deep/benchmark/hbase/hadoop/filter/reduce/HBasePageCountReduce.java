package com.stratio.deep.benchmark.hbase.hadoop.filter.reduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.stratio.deep.benchmark.common.hadoop.model.PageCountWritable;

public class HBasePageCountReduce
        extends
        Reducer<PageCountWritable, NullWritable, PageCountWritable, NullWritable> {

    @Override
    protected void reduce(PageCountWritable key, Iterable<NullWritable> values,
            Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
