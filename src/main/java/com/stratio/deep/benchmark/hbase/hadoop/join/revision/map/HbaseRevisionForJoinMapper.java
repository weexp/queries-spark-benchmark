package com.stratio.deep.benchmark.hbase.hadoop.join.revision.map;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.PageWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;

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
        String title = BenckmarkConstans.STRING_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_TITLE)) {
            title = Bytes.toString(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_TITLE));
        }
        Integer pageId = -1;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_ID)) {
            pageId = Bytes.toInt(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_ID));
        }
        Integer id = -1;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.ID)) {
            id = Bytes.toInt(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.ID));
        }
        Boolean isRedirect = false;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_IS_REDIRECT)) {
            isRedirect = Bytes.toBoolean(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_IS_REDIRECT));
        }
        String namespace = BenckmarkConstans.STRING_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_NS)) {
            namespace = Bytes.toString(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_NS));
        }
        String restrictions = BenckmarkConstans.STRING_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_RESTRICTIONS)) {
            restrictions = Bytes.toString(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_RESTRICTIONS));
        }
        String fullTitle = BenckmarkConstans.STRING_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_FULL_TITLE)) {
            fullTitle = Bytes.toString(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_FULL_TITLE));
        }
        PageWritable pageWritable = new PageWritable(namespace, title,
                fullTitle, pageId, isRedirect, restrictions);
        Boolean isMinor = false;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.REVISION_IS_MINOR)) {
            isMinor = Bytes.toBoolean(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.REVISION_IS_MINOR));
        }
        String text = BenckmarkConstans.STRING_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.REVISION_TEXT)) {
            text = Bytes.toString(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.REVISION_TEXT));
        }

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
        String redirection = BenckmarkConstans.STRING_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.REVISION_REDIRECTION)) {
            redirection = Bytes.toString(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.REVISION_REDIRECTION));
        }
        if (null == title || title.isEmpty()) {
            title = BenckmarkConstans.STRING_NULL;
            if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_COUNTER_TITLE)) {
                title = Bytes.toString(value.getValue(
                        BenckmarkConstans.COLUMN_FAMILY,
                        BenckmarkConstans.PAGE_COUNTER_TITLE));
            }
        }
        Long ts = BenckmarkConstans.LONG_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_COUNTER_TS)) {
            ts = Bytes.toLong(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_COUNTER_TS));
        }
        Integer pagecounts = BenckmarkConstans.INT_NULL;
        if (null != value.getValue(BenckmarkConstans.COLUMN_FAMILY,
                BenckmarkConstans.PAGE_COUNTER_COUNT)) {
            pagecounts = Bytes.toInt(value.getValue(
                    BenckmarkConstans.COLUMN_FAMILY,
                    BenckmarkConstans.PAGE_COUNTER_COUNT));
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
