package com.stratio.deep.benchmark.cassandra.spark.join;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionMapPageJoin implements
        PairFunction<Cells, String, Integer>, Serializable {

    @Override
    public Tuple2<String, Integer> call(Cells cells) throws Exception {
        Cell textCell = cells.getCellByName("title");
        String text = (String) textCell.getCellValue();
        Cell pageCountCell = cells.getCellByName("pagecounts");
        Integer pagecounts = (Integer) pageCountCell.getCellValue();
        return new Tuple2<String, Integer>(text, pagecounts);
    }
}
