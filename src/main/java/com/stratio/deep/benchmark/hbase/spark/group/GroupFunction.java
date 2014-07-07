package com.stratio.deep.benchmark.hbase.spark.group;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.deep.benchmark.BenckmarkConstans;

public class GroupFunction implements
        Function<Tuple2<String, Tuple2<Result, Result>>, String> {

    private static final long serialVersionUID = -4489927597929923942L;

    public String call(Tuple2<String, Tuple2<Result, Result>> v1)
            throws Exception {
        Tuple2<Result, Result> tuple = v1._2();
        String contributorName = null;
        if (null != tuple) {
            Result revisionResult = tuple._2();
            if (null != revisionResult) {
                contributorName = Bytes.toString(revisionResult.getValue(
                        BenckmarkConstans.COLUMN_FAMILY,
                        BenckmarkConstans.CONTRIBUTOR_USER_NAME));
            }
        }
        return contributorName;
    }
}
