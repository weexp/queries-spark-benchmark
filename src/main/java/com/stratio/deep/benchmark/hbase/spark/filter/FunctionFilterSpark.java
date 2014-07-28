package com.stratio.deep.benchmark.hbase.spark.filter;

import java.util.Calendar;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.deep.benchmark.common.BenchmarkConstans;

public class FunctionFilterSpark implements
        Function<Tuple2<ImmutableBytesWritable, Result>, Boolean> {

    /**
     * 
     */
    private static final long serialVersionUID = 1137120295128520072L;

    @Override
    public Boolean call(Tuple2<ImmutableBytesWritable, Result> v1)
            throws Exception {
        Result result = v1._2();
        byte[] ts = result.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_COUNTER_TS);
        if (null != ts) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(Bytes.toLong(ts));
            int hour = cal.get(Calendar.HOUR_OF_DAY);
            return hour >= 19 && hour < 20;
        }
        return false;
    }
}
