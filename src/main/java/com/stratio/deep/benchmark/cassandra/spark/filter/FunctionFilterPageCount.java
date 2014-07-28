package com.stratio.deep.benchmark.cassandra.spark.filter;

import java.util.Calendar;
import java.util.Date;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;

/**
 * Created by ParadigmaTecnologico on 23/06/2014.
 */
public class FunctionFilterPageCount implements Function<Cells, Boolean> {

    @Override
    public Boolean call(Cells cells) throws Exception {

        Cell tsCell = cells.getCellByName("ts");
        Date ts = (Date) tsCell.getCellValue();
        Calendar cal = Calendar.getInstance();
        cal.setTime(ts);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        // String hora = (String.valueOf(hour));
        // String hora = hour < 10?
        // "0".concat(String.valueOf(hour)):String.valueOf(hour);
        // if ("3".equals(String.valueOf(hour))) &&
        // ("3".equals(String.valueOf(hour)))
        // if (hour >= 19)
        return hour >= 19 && hour < 20;
    }
}
