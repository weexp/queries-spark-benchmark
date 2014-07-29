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
        //Calendar cal = Calendar.getInstance();
        //cal.setTime(ts);
        //int hour = cal.get(Calendar.HOUR_OF_DAY);
        // String hora = (String.valueOf(hour));
        // String hora = hour < 10?
        // "0".concat(String.valueOf(hour)):String.valueOf(hour);
        // if ("3".equals(String.valueOf(hour))) &&
        // ("3".equals(String.valueOf(hour)))
        // if (hour >= 19)
        return numPage >= 2 && numPage < 4;
    }
}
