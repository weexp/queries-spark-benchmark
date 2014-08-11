package com.stratio.deep.benchmark.cassandra.spark.join;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.datastax.spark.connector.CassandraRow;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionDatastaxtMapRevJoin implements Serializable,
        PairFunction<CassandraRow, String, String> {

    /**
     * 
     */
    private static final long serialVersionUID = -8944489888162957403L;

    @Override
    public Tuple2<String, String> call(CassandraRow row) throws Exception {
        String text = row.getString("page_title");
        String contributor = row.getString("contributor_username");
        return new Tuple2<String, String>(text, contributor);
    }
}
