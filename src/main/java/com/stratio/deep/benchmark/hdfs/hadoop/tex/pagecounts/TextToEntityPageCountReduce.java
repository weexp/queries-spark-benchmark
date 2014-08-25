package com.stratio.deep.benchmark.hdfs.hadoop.tex.pagecounts;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.stratio.deep.benchmark.common.hadoop.model.PageCountWritable;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;

public class TextToEntityPageCountReduce
        extends
        Reducer<UUIDWritable, PageCountWritable, UUIDWritable, PageCountWritable> {

    @Override
    protected void reduce(UUIDWritable key, Iterable<PageCountWritable> values,
            Context context) throws IOException, InterruptedException {
        System.out.println("Enter in the reducer");
        for (PageCountWritable value : values) {
            context.write(key, value);
        }
    }
}
