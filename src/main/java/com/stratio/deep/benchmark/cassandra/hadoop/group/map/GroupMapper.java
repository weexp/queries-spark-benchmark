package com.stratio.deep.benchmark.cassandra.hadoop.group.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.model.ContributorWritable;

public class GroupMapper
        extends
        Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, ContributorWritable, NullWritable> {

    @Override
    protected void map(Map<String, ByteBuffer> key,
            Map<String, ByteBuffer> value, Context context) throws IOException,
            InterruptedException {
        Integer contributorId = -1;
        ByteBuffer contributorIdBB = value
                .get(BenckmarkConstans.CONTRIBUTOR_ID);
        if (null != contributorIdBB) {
            contributorId = Int32Type.instance.compose(contributorIdBB);
        }
        String username = BenckmarkConstans.STRING_NULL;
        ByteBuffer usernameBB = value
                .get(BenckmarkConstans.CONTRIBUTOR_USER_NAME);
        if (null != usernameBB) {
            username = UTF8Type.instance.compose(usernameBB);
        }
        Boolean isAnonymous = false;
        ByteBuffer isAnonymousBB = value
                .get(BenckmarkConstans.CONTRIBUTOR_IS_ANONYMOUS);
        if (null != isAnonymousBB) {
            isAnonymous = BooleanType.instance.compose(isAnonymousBB);
        }
        context.write(new ContributorWritable(contributorId, username,
                isAnonymous), NullWritable.get());
    }

}
