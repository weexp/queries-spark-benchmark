package com.stratio.deep.benchmark.hbase.spark.group;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.hbase.serialize.ResultSerializable;

public class GroupFunction implements Function<ResultSerializable, String> {

    /**
     * 
     */
    private static final long serialVersionUID = -2436366003475123778L;

    @Override
    public String call(ResultSerializable v1) throws Exception {

        return (String) v1.getValue(BenckmarkConstans.COLUMN_FAMILY_NAME,
                BenckmarkConstans.CONTRIBUTOR_USERNAME_COLUMN_NAME);
    }

}
