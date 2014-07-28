package com.stratio.deep.benchmark.common.hadoop.counter.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;

public class CounterMap<K, V> extends Mapper<K, V, IntWritable, NullWritable> {

    @Override
    protected void map(K key, V value, Context context) throws IOException,
            InterruptedException {
        context.write(BenchmarkConstans.KEY_COUNTERS, NullWritable.get());
    }

}
