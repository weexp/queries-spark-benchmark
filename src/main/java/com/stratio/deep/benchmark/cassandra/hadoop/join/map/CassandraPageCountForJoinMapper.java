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

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.PageWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;

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
        String title = BenchmarkConstans.STRING_NULL;
        ByteBuffer titleBB = value
                .get(BenchmarkConstans.PAGE_COUNTER_TITLE);
        if (null != titleBB) {
            title = UTF8Type.instance.compose(titleBB);
        }
        Long ts = BenchmarkConstans.LONG_NULL;
        ByteBuffer tsBB = value.get(BenchmarkConstans.PAGE_COUNTER_TS);
        if (null != tsBB) {
            ts = LongType.instance.compose(tsBB);
        }
        Integer pagecounts = BenchmarkConstans.INT_NULL;
        ByteBuffer pageCountsBB = value
                .get(BenchmarkConstans.PAGE_COUNTER_COUNT);
        if (null != pageCountsBB) {
            pagecounts = Int32Type.instance.compose(pageCountsBB);
        }
        context.write(new Text(title), new RevisionPageCounter(title, new Date(
                ts), pagecounts, new ContributorWritable(),
                BenchmarkConstans.BOOLEAN_NULL, new PageWritable(),
                BenchmarkConstans.STRING_NULL,
                BenchmarkConstans.STRING_NULL));
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
}
