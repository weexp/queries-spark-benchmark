package com.stratio.deep.benchmark.hbase.spark.map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.hbase.serialize.ResultSerializable;

public class MapRevisionFunction implements
        Function<Tuple2<ImmutableBytesWritable, Result>, ResultSerializable> {

    /**
     * 
     */
    private static final long serialVersionUID = -7467004678412540149L;

    @Override
    public ResultSerializable call(Tuple2<ImmutableBytesWritable, Result> t)
            throws Exception {
        return ResultSerializable.builder(t._2(),
                BenchmarkConstans.REVISION_METADATA);
    }
}
