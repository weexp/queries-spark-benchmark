package com.stratio.deep.benchmark.cassandra.spark.filter;

import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.CassandraRow;

/**
 * Created by ParadigmaTecnologico on 23/06/2014.
 */
public class FunctionFilterDatastaxtPageCount implements
        Function<CassandraRow, Boolean> {

    @Override
    public Boolean call(CassandraRow row) throws Exception {
        /*
        Integer numPage = row.getInt("page_id");
        return numPage >= 18295009 && numPage < 18295020;
        //return numPage < 36283;
        */
        Integer numPage = row.getInt("pagecounts");
        return numPage >= 199 && numPage < 201;

    }
}
