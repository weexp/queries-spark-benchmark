package com.stratio.deep.benchmark.hbase.hadoop.join.map;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.PageWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;

public class HbaseRevisionForJoinMapper extends
        TableMapper<Text, RevisionPageCounter> {

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        String title = BenchmarkConstans.STRING_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_TITLE)) {
            title = Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.PAGE_TITLE));
        }
        Integer pageId = -1;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_ID)) {
            pageId = Bytes
                    .toInt(value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                            BenchmarkConstans.PAGE_ID));
        }
        UUID id = null;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.ID)) {
            id = UUID.fromString(Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY, BenchmarkConstans.ID)));
        }
        Boolean isRedirect = false;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_IS_REDIRECT)) {
            isRedirect = Bytes.toBoolean(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.PAGE_IS_REDIRECT));
        }
        String namespace = BenchmarkConstans.STRING_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_NS)) {
            namespace = Bytes
                    .toString(value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                            BenchmarkConstans.PAGE_NS));
        }
        String restrictions = BenchmarkConstans.STRING_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_RESTRICTIONS)) {
            restrictions = Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.PAGE_RESTRICTIONS));
        }
        String fullTitle = BenchmarkConstans.STRING_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_FULL_TITLE)) {
            fullTitle = Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.PAGE_FULL_TITLE));
        }
        PageWritable pageWritable = new PageWritable(namespace, title,
                fullTitle, pageId, isRedirect, new Text(restrictions));
        Boolean isMinor = false;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.REVISION_IS_MINOR)) {
            isMinor = Bytes.toBoolean(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.REVISION_IS_MINOR));
        }
        String text = BenchmarkConstans.STRING_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.REVISION_TEXT)) {
            text = Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.REVISION_TEXT));
        }

        Integer contributorId = -1;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.CONTRIBUTOR_ID)) {
            contributorId = Bytes.toInt(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.CONTRIBUTOR_ID));
        }
        String username = BenchmarkConstans.STRING_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.CONTRIBUTOR_USER_NAME)) {
            username = Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.CONTRIBUTOR_USER_NAME));
        }
        Boolean isAnonymous = false;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.CONTRIBUTOR_IS_ANONYMOUS)) {
            isAnonymous = Bytes.toBoolean(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.CONTRIBUTOR_IS_ANONYMOUS));
        }
        ContributorWritable contributorWritable = new ContributorWritable(
                contributorId, username, isAnonymous);
        String redirection = BenchmarkConstans.STRING_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.REVISION_REDIRECTION)) {
            redirection = Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.REVISION_REDIRECTION));
        }
        if (null == title || title.isEmpty()) {
            title = BenchmarkConstans.STRING_NULL;
            if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.PAGE_COUNTER_TITLE)) {
                title = Bytes.toString(value.getValue(
                        BenchmarkConstans.COLUMN_FAMILY,
                        BenchmarkConstans.PAGE_COUNTER_TITLE));
            }
        }
        Long ts = BenchmarkConstans.LONG_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_COUNTER_TS)) {
            ts = Bytes.toLong(value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.PAGE_COUNTER_TS));
        }
        Integer pagecounts = BenchmarkConstans.INT_NULL;
        if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
                BenchmarkConstans.PAGE_COUNTER_COUNT)) {
            pagecounts = Integer.valueOf(Bytes.toString(value.getValue(
                    BenchmarkConstans.COLUMN_FAMILY,
                    BenchmarkConstans.PAGE_COUNTER_COUNT)));
        }

        context.write(new Text(pageWritable.getTitle()),
                new RevisionPageCounter(title, new Date(ts), pagecounts,
                        contributorWritable, isMinor, pageWritable, text,
                        redirection));
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

}
