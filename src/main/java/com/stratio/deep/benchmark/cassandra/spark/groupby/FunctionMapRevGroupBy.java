package com.stratio.deep.benchmark.cassandra.spark.groupby;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.stratio.deep.entity.Cells;

/**
 * Created by ParadigmaTecnologico on 23/06/2014.
 */
public class FunctionMapRevGroupBy implements
        PairFunction<Tuple2<String, Iterable<Cells>>, String, Integer>,
        Serializable {

    @Override
    public Tuple2<String, Integer> call(Tuple2<String, Iterable<Cells>> t)
            throws Exception {
        String contributor = t._1();
        int i = 0;
        for (Cells cells : t._2()) {
            i++;
        }
        return new Tuple2<String, Integer>(contributor, i);
    }
}
