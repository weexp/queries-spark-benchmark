package com.stratio.deep.benchmark.cassandra.hadoop.join.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.PageWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;

public class CassandraRevisionForJoinMapper
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
        ByteBuffer pageTitleBB = value
                .get(BenchmarkConstans.PAGE_TITLE_COLUMN_NAME);
        if (null != pageTitleBB) {
            title = UTF8Type.instance.compose(pageTitleBB);
        }
        Integer pageId = -1;
        ByteBuffer pageIdBB = value.get(BenchmarkConstans.PAGE_ID_COLUMN_NAME);
        if (null != pageIdBB) {
            pageId = Int32Type.instance.compose(pageIdBB);
        }
        UUID id = null;
        ByteBuffer idBB = value.get(BenchmarkConstans.ID_COLUMN_NAME);
        if (null != idBB) {
            id = UUIDType.instance.compose(idBB);
        }
        Boolean isRedirect = false;
        ByteBuffer isRedirectBB = value
                .get(BenchmarkConstans.PAGE_ISREDIRECT_COLUMN_NAME);
        if (null != isRedirectBB) {
            isRedirect = BooleanType.instance.compose(idBB);
        }
        String namespace = BenchmarkConstans.STRING_NULL;
        ByteBuffer pageNsBB = value.get(BenchmarkConstans.PAGE_NS_COLUMN_NAME);
        if (null != pageNsBB) {
            namespace = UTF8Type.instance.compose(pageNsBB);
        }
        String restrictions = BenchmarkConstans.STRING_NULL;
        ByteBuffer restrictionsBB = value
                .get(BenchmarkConstans.PAGE_RESTRICTIONS_COLUMN_NAME);
        if (null != restrictionsBB) {
            restrictions = UTF8Type.instance.compose(restrictionsBB);
        }
        String fullTitle = BenchmarkConstans.STRING_NULL;
        ByteBuffer fullTitleBB = value
                .get(BenchmarkConstans.PAGE_FULL_TITLE_COLUMN_NAME);
        if (null != fullTitleBB) {
            fullTitle = UTF8Type.instance.compose(fullTitleBB);
        }
        PageWritable pageWritable = new PageWritable(namespace, title,
                fullTitle, pageId, isRedirect, new Text(restrictions));
        Boolean isMinor = false;
        ByteBuffer isMinorBB = value
                .get(BenchmarkConstans.REVISION_ISMINOR_COLUMN_NAME);
        if (null != isMinorBB) {
            isMinor = BooleanType.instance.compose(value
                    .get(BenchmarkConstans.REVISION_ISMINOR_COLUMN_NAME));
        }
        String text = BenchmarkConstans.STRING_NULL;
        ByteBuffer textBB = value
                .get(BenchmarkConstans.REVISION_TEXT_COLUMN_NAME);
        if (null != textBB) {
            text = UTF8Type.instance.compose(textBB);
        }

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
        ContributorWritable contributorWritable = new ContributorWritable(
                contributorId, username, isAnonymous);
        String redirection = BenchmarkConstans.STRING_NULL;
        if (null != value
                .get(BenchmarkConstans.REVISION_REDIRECTION_COLUMN_NAME)) {
            redirection = UTF8Type.instance.compose(value
                    .get(BenchmarkConstans.REVISION_REDIRECTION_COLUMN_NAME));
        }
        context.write(new Text(pageWritable.getTitle()),
                new RevisionPageCounter(title, BenchmarkConstans.DATE_NULL,
                        BenchmarkConstans.INT_NULL, contributorWritable,
                        isMinor, pageWritable, text, redirection));
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
}
