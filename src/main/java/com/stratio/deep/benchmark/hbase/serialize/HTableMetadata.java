package com.stratio.deep.benchmark.hbase.serialize;

import java.util.Arrays;
import java.util.List;

public class HTableMetadata {

    List<HColumnFamilyMetadata> columnsFamiliyMetaData;
    String name;

    public HTableMetadata(String name,
            HColumnFamilyMetadata... columnsFamiliyMetaData) {
        this.name = name;
        this.columnsFamiliyMetaData = Arrays.asList(columnsFamiliyMetaData);
    }

    public List<HColumnFamilyMetadata> getColumnsFamiliyMetaData() {
        return this.columnsFamiliyMetaData;
    }

    public void setColumnsFamiliyMetaData(
            List<HColumnFamilyMetadata> columnsFamiliyMetaData) {
        this.columnsFamiliyMetaData = columnsFamiliyMetaData;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
