package com.stratio.deep.benchmark.hbase.serialize;

public class HQualifiersMetadata {

    private final String name;
    private final DataType dataType;

    public HQualifiersMetadata(String name, DataType dataType) {
        super();
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return this.name;
    }

    public DataType getDataType() {
        return this.dataType;
    }

}
