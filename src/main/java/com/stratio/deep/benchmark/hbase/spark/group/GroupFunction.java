package com.stratio.deep.benchmark.hbase.spark.group;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.hbase.serialize.ResultSerializable;

public class GroupFunction implements Function<ResultSerializable, String> {

    private static final long serialVersionUID = -4489927597929923942L;

    @Override
    public String call(ResultSerializable v1) throws Exception {
        // TODO Auto-generated method stub
        ResultSerializable revisionResult = v1;
        String contributorName = null;
        if (null != revisionResult) {
            contributorName = (String) revisionResult.getValue(
                    BenckmarkConstans.COLUMN_FAMILY_NAME,
                    BenckmarkConstans.CONTRIBUTOR_USERNAME_COLUMN_NAME);
        }

        return contributorName;
    }
}
