package com.stratio.deep.benchmark.cassandra.hadoop.group.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;

public class GroupMapper
        extends
        Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, ContributorWritable, NullWritable> {

    @Override
    protected void map(Map<String, ByteBuffer> key,
            Map<String, ByteBuffer> value, Context context) throws IOException,
            InterruptedException {
        Integer contributorId = -1;
        ByteBuffer contributorIdBB = value
                .get(BenchmarkConstans.CONTRIBUTOR_ID_COLUMN_NAME);
        if (null != contributorIdBB) {
            contributorId = Int32Type.instance.compose(contributorIdBB);
        }
        String username = BenchmarkConstans.STRING_NULL;
        ByteBuffer usernameBB = value
                .get(BenchmarkConstans.CONTRIBUTOR_USERNAME_COLUMN_NAME);
        if (null != usernameBB) {
            username = UTF8Type.instance.compose(usernameBB);
        }
        Boolean isAnonymous = false;
        ByteBuffer isAnonymousBB = value
                .get(BenchmarkConstans.CONTRIBUTOR_ISANONYMOUS_COLUMN_NAME);
        if (null != isAnonymousBB) {
            isAnonymous = BooleanType.instance.compose(isAnonymousBB);
        }
        context.write(new ContributorWritable(contributorId, username,
                isAnonymous), NullWritable.get());
    }

}
