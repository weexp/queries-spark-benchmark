package com.stratio.deep.benchmark.cassandra.hadoop.join.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.PageWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;

public class CassandraPageCountForJoinMapper
        extends
        Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, RevisionPageCounter> {

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(Map<String, ByteBuffer> key,
            Map<String, ByteBuffer> value, Context context) throws IOException,
            InterruptedException {
        String title = BenckmarkConstans.STRING_NULL;
        ByteBuffer titleBB = value
                .get(BenckmarkConstans.PAGE_COUNTER_TITLE);
        if (null != titleBB) {
            title = UTF8Type.instance.compose(titleBB);
        }
        Long ts = BenckmarkConstans.LONG_NULL;
        ByteBuffer tsBB = value.get(BenckmarkConstans.PAGE_COUNTER_TS);
        if (null != tsBB) {
            ts = LongType.instance.compose(tsBB);
        }
        Integer pagecounts = BenckmarkConstans.INT_NULL;
        ByteBuffer pageCountsBB = value
                .get(BenckmarkConstans.PAGE_COUNTER_COUNT);
        if (null != pageCountsBB) {
            pagecounts = Int32Type.instance.compose(pageCountsBB);
        }
        context.write(new Text(title), new RevisionPageCounter(title, new Date(
                ts), pagecounts, new ContributorWritable(),
                BenckmarkConstans.BOOLEAN_NULL, new PageWritable(),
                BenckmarkConstans.STRING_NULL,
                BenckmarkConstans.STRING_NULL));
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
}
