package com.stratio.deep.benchmark.hbase.spark.group;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.stratio.deep.benchmark.hbase.serialize.ResultSerializable;

public class MapToGroupFunction
        implements
        PairFunction<Tuple2<String, Iterable<ResultSerializable>>, String, Integer> {

    /**
     * 
     */
    private static final long serialVersionUID = 3720535488081527420L;

    @Override
    public Tuple2<String, Integer> call(
            Tuple2<String, Iterable<ResultSerializable>> t) throws Exception {
        int i = 0;
        for (@SuppressWarnings("unused")
        ResultSerializable res : t._2()) {
            i++;
        }
        return new Tuple2<String, Integer>(t._1(), i);
    }
}
