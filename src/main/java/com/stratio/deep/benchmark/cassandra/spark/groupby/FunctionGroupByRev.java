package com.stratio.deep.benchmark.cassandra.spark.groupby;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionGroupByRev implements Function<Cells, String>,
        Serializable {

    @Override
    public String call(Cells cells) throws Exception {
        Cell autorCell = cells.getCellByName("contributor_username");
        String autor = (String) autorCell.getCellValue();
        return autor;
    }
}
