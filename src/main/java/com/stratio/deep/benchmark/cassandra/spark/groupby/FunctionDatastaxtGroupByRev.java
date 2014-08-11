package com.stratio.deep.benchmark.cassandra.spark.groupby;

import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.CassandraRow;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionDatastaxtGroupByRev implements
        Function<CassandraRow, String> {

    private final String cellName = "contributor_username";
    private static final long serialVersionUID = -58288997899038056L;

    @Override
    public String call(CassandraRow row) throws Exception {
        return row.getString(this.cellName);
    }
}
