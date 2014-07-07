package com.stratio.deep.benchmark.hbase.spark.map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.stratio.deep.benchmark.BenckmarkConstans;

public class MapRevisionFunction implements
        PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Result> {

    /**
     * 
     */
    private static final long serialVersionUID = -7467004678412540149L;

    public Tuple2<String, Result> call(Tuple2<ImmutableBytesWritable, Result> t)
            throws Exception {
        Result result = t._2();
        String title = Bytes.toString(result.getValue(
                BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_TITLE));
        return new Tuple2<String, Result>(title, result);
    }

}
