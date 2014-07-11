package com.stratio.deep.benchmark.hbase.hadoop.group.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.model.ContributorWritable;

public class HbaseGroupMap
        extends
        Mapper<ImmutableBytesWritable, Result, ContributorWritable, NullWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) {
        Integer contributorId = -1;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.CONTRIBUTOR_ID)) {
            contributorId = Bytes.toInt(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.CONTRIBUTOR_ID));
        }
        String username = BenckmarkConstans.STRING_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.CONTRIBUTOR_USER_NAME)) {
            username = Bytes.toString(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.CONTRIBUTOR_USER_NAME));
        }
        Boolean isAnonymous = false;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.CONTRIBUTOR_IS_ANONYMOUS)) {
            isAnonymous = Bytes.toBoolean(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.CONTRIBUTOR_IS_ANONYMOUS));
        }
        ContributorWritable contributorWritable = new ContributorWritable(
                contributorId, username, isAnonymous);

        try {
            context.write(contributorWritable, NullWritable.get());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
