package com.stratio.deep;

import com.stratio.deep.entity.Cells;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by ParadigmaTecnologico on 23/06/2014.
 */
public class FunctionMapRevGroupBy extends PairFunction<Tuple2<String, List<Cells>>, String, Integer> implements Serializable {

    @Override
    public Tuple2<String, Integer> call(Tuple2<String, List<Cells>> t) throws Exception {
        return new Tuple2<String,Integer>(t._1(), t._2().size());
    }
}
