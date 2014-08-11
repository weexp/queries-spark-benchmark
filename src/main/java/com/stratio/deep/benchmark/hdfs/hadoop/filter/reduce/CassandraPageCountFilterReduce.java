package com.stratio.deep.benchmark.hdfs.hadoop.filter.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.stratio.deep.benchmark.common.hadoop.model.PageCountWritable;

public class CassandraPageCountFilterReduce extends
        Reducer<PageCountWritable, IntWritable, PageCountWritable, IntWritable> {

    @Override
    protected void reduce(PageCountWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int i = 0;
        for (@SuppressWarnings("unused")
        IntWritable value : values) {
            i++;
        }
        context.write(key, new IntWritable(i));
    }
}
