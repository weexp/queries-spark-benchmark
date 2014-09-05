package com.stratio.deep.benchmark.cassandra.spark.filter;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import org.apache.spark.api.java.function.Function;

/**
 * Created by ParadigmaTecnologico on 23/06/2014.
 */
public class FunctionFilterPageCount implements Function<Cells, Boolean> {

    @Override
    public Boolean call(Cells cells) throws Exception {


        Cell pageCell = cells.getCellByName("pagecounts");
        Integer numPage = (Integer) pageCell.getCellValue();
        return numPage >= 199 && numPage < 201;
        /*
        Cell pageCell = cells.getCellByName("page_id");
        Integer numPage = (Integer) pageCell.getCellValue();
        return numPage >= 18295009 && numPage < 18295020;
        //return numPage < 36283;
        */
    }
}
