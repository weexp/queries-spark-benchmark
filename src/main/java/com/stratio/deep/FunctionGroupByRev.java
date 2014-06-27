package com.stratio.deep;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionGroupByRev extends Function<Cells, String> implements Serializable{
    @Override
    public String call(Cells cells) throws Exception {
        Cell autorCell = cells.getCellByName("contributor_username");
        String autor =(String) autorCell.getCellValue();
        return autor;
    }
}
