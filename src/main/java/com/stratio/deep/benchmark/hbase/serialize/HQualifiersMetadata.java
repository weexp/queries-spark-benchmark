package com.stratio.deep.benchmark.hbase.serialize;

public class HQualifiersMetadata {

    private String name;
    private DataType dataType;

    public HQualifiersMetadata(String name, DataType dataType) {
        super();
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

}
