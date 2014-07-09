package com.stratio.deep.benchmark.cassandra.hadoop.filter.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.cassandra.db.marshal.DateType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.BenckmarkConstans;

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

        Date ts = BenckmarkConstans.DATE_NULL;
        ByteBuffer tsBB = value.get(BenckmarkConstans.PAGE_COUNTER_TS);
        if (null != tsBB) {
            ts = DateType.instance.compose(tsBB);
            Calendar cal = Calendar.getInstance();
            cal.setTime(ts);
            if (cal.get(Calendar.HOUR_OF_DAY) >= 3
                    && cal.get(Calendar.HOUR_OF_DAY) <= 4) {
                context.write(this.keyToSend, this.valueToSend);
            }
        }
    }

}