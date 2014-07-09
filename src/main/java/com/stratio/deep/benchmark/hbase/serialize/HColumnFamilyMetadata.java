package com.stratio.deep.benchmark.hbase.serialize;

import java.util.Arrays;
import java.util.List;

public class HColumnFamilyMetadata {

    private String name;
    private List<HQualifiersMetadata> qualifiers;

    public HColumnFamilyMetadata(String name, HQualifiersMetadata... qualifiers) {
        super();
        this.name = name;
        this.qualifiers = Arrays.asList(qualifiers);
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<HQualifiersMetadata> getQualifiers() {
        return this.qualifiers;
    }

    public void setQualifiers(List<HQualifiersMetadata> qualifiers) {
        this.qualifiers = qualifiers;
    }

}
