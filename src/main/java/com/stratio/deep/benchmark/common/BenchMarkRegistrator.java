package com.stratio.deep.benchmark.common;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.stratio.deep.benchmark.hbase.serialize.ResultSerializable;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;

public class BenchMarkRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(ResultSerializable.class);
        kryo.register(Cells.class);
        kryo.register(Cell.class);
    }

}
