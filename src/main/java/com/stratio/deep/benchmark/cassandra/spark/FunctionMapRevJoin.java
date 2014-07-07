package com.stratio.deep.benchmark.cassandra.spark;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;

/**
 * Created by ParadigmaTecnologico on 24/06/2014.
 */
public class FunctionMapRevJoin implements Serializable,
        PairFunction<Cells, String, String> {

    @Override
    public Tuple2<String, String> call(Cells cells) throws Exception {
        Cell textCell = cells.getCellByName("page_title");
        String text = (String) textCell.getCellValue();
        Cell contributorCell = cells.getCellByName("contributor_username");
        String contributor = (String) contributorCell.getCellValue();
        return new Tuple2<String, String>(text, contributor);
    }
}
