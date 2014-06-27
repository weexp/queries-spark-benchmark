package com.stratio.deep;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import org.apache.spark.api.java.function.Function;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by ParadigmaTecnologico on 23/06/2014.
 */
public class FunctionFilterPageCount extends Function<Cells,Boolean> {
    @Override
    public Boolean call(Cells cells) throws Exception {

        Cell tsCell = cells.getCellByName("ts");
        Date ts = (Date) tsCell.getCellValue();
        Calendar cal= Calendar.getInstance();
        cal.setTime(ts);
        int hour=cal.get(Calendar.HOUR_OF_DAY);
        //String hora = (String.valueOf(hour));
        //String hora = hour < 10? "0".concat(String.valueOf(hour)):String.valueOf(hour);
        //if  ("3".equals(String.valueOf(hour))) && ("3".equals(String.valueOf(hour)))
        //if (hour >= 19)
        if ((hour >=19) && (hour <20))
            return true;
        else return false;

    }
}
