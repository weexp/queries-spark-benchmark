package com.stratio.deep.benchmark.hbase.spark.map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.hbase.serialize.ResultSerializable;

public class MapPageCountFunction
        implements
        PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, ResultSerializable> {

    /**
     * 
     */
    private static final long serialVersionUID = -7467004678412540149L;

    @Override
    public Tuple2<String, ResultSerializable> call(
            Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
        ResultSerializable resultSerializable = this
                .convertPageConterResulToSerializable(t._2());
        String title = (String) resultSerializable.getValue(
                BenchmarkConstans.COLUMN_FAMILY_NAME,
                BenchmarkConstans.PAGE_COUNTER_TITLE_COLUMN_NAME);
        return new Tuple2<String, ResultSerializable>(title, resultSerializable);
    }

    private ResultSerializable convertPageConterResulToSerializable(
            Result result) {
        return ResultSerializable.builder(result,
                BenchmarkConstans.PAGE_COUNT_METADATA);
    }
}
