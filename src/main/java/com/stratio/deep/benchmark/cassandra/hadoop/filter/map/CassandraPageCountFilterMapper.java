package com.stratio.deep.benchmark.cassandra.hadoop.filter.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;

public class CassandraPageCountFilterMapper
        extends
        Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, IntWritable, NullWritable> {

    IntWritable keyToSend = null;
    NullWritable valueToSend = null;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        this.keyToSend = new IntWritable(1);
        this.valueToSend = NullWritable.get();
        super.setup(context);
    }

    @Override
    protected void map(Map<String, ByteBuffer> key,
            Map<String, ByteBuffer> value, Context context) throws IOException,
            InterruptedException {

        Long counter = BenchmarkConstans.LONG_NULL;
        ByteBuffer tsBB = value.get(BenchmarkConstans.PAGE_COUNTER_COUNT);
        if (null != tsBB) {
            counter = LongType.instance.compose(tsBB);
            if (counter >= 1 && counter < 4) {
                context.write(this.keyToSend, this.valueToSend);
            }
        }
    }

}
