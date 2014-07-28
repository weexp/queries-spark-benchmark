package com.stratio.deep.benchmark.hbase.serialize;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.stratio.deep.benchmark.common.BenchmarkConstans;

public class ResultSerializable implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7612263057917993564L;
    final Map<String, Map<String, Object>> hTable;

    private ResultSerializable(Map<String, Map<String, Object>> hTable) {
        this.hTable = hTable;
    }

    static public ResultSerializable builder(Result result,
            HTableMetadata metaData) {
        Map<String, Map<String, Object>> hTable = new HashMap<>();
        for (HColumnFamilyMetadata columnFamily : metaData
                .getColumnsFamiliyMetaData()) {
            String columnFamilyName = columnFamily.getName();
            Map<String, Object> columnFamilyMap = hTable.get(columnFamilyName);
            if (null == columnFamilyMap) {
                columnFamilyMap = new HashMap<String, Object>();
            }
            for (HQualifiersMetadata qualifier : columnFamily.getQualifiers()) {
                String qualifierName = qualifier.getName();
                columnFamilyMap.put(qualifierName, DataConverter.convert(
                        qualifier.getDataType(),
                        result.getValue(BenchmarkConstans.COLUMN_FAMILY,
                                Bytes.toBytes(qualifierName))));
            }
            hTable.put(columnFamilyName, columnFamilyMap);
        }
        return new ResultSerializable(hTable);
    }

    public Object getValue(String columnFamily, String qualifierName) {
        return this.hTable.get(columnFamily).get(qualifierName);
    }
}
