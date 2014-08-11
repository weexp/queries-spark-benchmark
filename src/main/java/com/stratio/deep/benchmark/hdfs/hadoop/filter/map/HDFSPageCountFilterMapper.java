package com.stratio.deep.benchmark.hdfs.hadoop.filter.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.PageCountWritable;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;

public class HDFSPageCountFilterMapper extends
        Mapper<UUIDWritable, PageCountWritable, PageCountWritable, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(UUIDWritable key, PageCountWritable value,
            Context context) throws IOException, InterruptedException {
        Integer pageCount = value.getPageCount();
        if (null != pageCount && pageCount != BenchmarkConstans.INT_NULL) {
            if (pageCount >= 2 && pageCount < 4) {
                context.write(value, new IntWritable(pageCount));
            }
        }
    }

}
