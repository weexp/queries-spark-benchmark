package com.stratio.deep.benchmark.cassandra.spark.join;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.datastax.spark.connector.CassandraRow;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionMapDatastaxtPageJoin implements
        PairFunction<CassandraRow, String, Integer>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -8246444842949644374L;

    @Override
    public Tuple2<String, Integer> call(CassandraRow row) throws Exception {
        String text = row.getString("title");
        Integer pagecounts = row.getInt("pagecounts");
        return new Tuple2<String, Integer>(text, pagecounts);
    }
}
