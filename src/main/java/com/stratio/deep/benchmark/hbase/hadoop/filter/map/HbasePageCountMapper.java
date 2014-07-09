package com.stratio.deep.benchmark.hbase.hadoop.filter.map;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import com.stratio.deep.benchmark.BenckmarkConstans;

public class HbasePageCountMapper extends
        TableMapper<IntWritable, NullWritable> {

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
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {

        Long ts = BenckmarkConstans.LONG_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_COUNT_TS)) {
            ts = Bytes.toLong(value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_COUNT_TS));
        }
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(ts);
        if (cal.get(Calendar.HOUR_OF_DAY) >= 3
                && cal.get(Calendar.HOUR_OF_DAY) <= 4) {
            context.write(this.keyToSend, this.valueToSend);
        }
    }

}
