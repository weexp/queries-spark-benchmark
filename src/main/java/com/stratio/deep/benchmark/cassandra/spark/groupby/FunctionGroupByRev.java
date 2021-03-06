package com.stratio.deep.benchmark.cassandra.spark.groupby;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.entity.Cells;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionGroupByRev implements Function<Cells, String> {

    private final String cellName = "contributor_username";
    private static final long serialVersionUID = -58288997899038056L;

    @Override
    public String call(Cells cells) throws Exception {
        return (String) cells.getCellByName(this.cellName).getCellValue();
    }
}
