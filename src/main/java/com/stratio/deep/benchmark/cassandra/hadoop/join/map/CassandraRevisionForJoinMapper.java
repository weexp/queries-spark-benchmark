package com.stratio.deep.benchmark.cassandra.hadoop.join.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.PageWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;

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
        String title = BenckmarkConstans.STRING_NULL;
        ByteBuffer pageTitleBB = value
                .get(BenckmarkConstans.PAGE_TITLE_COLUMN_NAME);
        if (null != pageTitleBB) {
            title = UTF8Type.instance.compose(pageTitleBB);
        }
        Integer pageId = -1;
        ByteBuffer pageIdBB = value
                .get(BenckmarkConstans.PAGE_ID_COLUMN_NAME);
        if (null != pageIdBB) {
            pageId = Int32Type.instance.compose(pageIdBB);
        }
        Integer id = -1;
        ByteBuffer idBB = value.get(BenckmarkConstans.ID);
        if (null != idBB) {
            id = Int32Type.instance.compose(idBB);
        }
        Boolean isRedirect = false;
        ByteBuffer isRedirectBB = value
                .get(BenckmarkConstans.PAGE_ISREDIRECT_COLUMN_NAME);
        if (null != isRedirectBB) {
            isRedirect = BooleanType.instance.compose(idBB);
        }
        String namespace = BenckmarkConstans.STRING_NULL;
        ByteBuffer pageNsBB = value
                .get(BenckmarkConstans.PAGE_NS_COLUMN_NAME);
        if (null != pageNsBB) {
            namespace = UTF8Type.instance.compose(pageNsBB);
        }
        String restrictions = BenckmarkConstans.STRING_NULL;
        ByteBuffer restrictionsBB = value
                .get(BenckmarkConstans.PAGE_RESTRICTIONS_COLUMN_NAME);
        if (null != restrictionsBB) {
            restrictions = UTF8Type.instance.compose(restrictionsBB);
        }
        String fullTitle = BenckmarkConstans.STRING_NULL;
        ByteBuffer fullTitleBB = value
                .get(BenckmarkConstans.PAGE_FULL_TITLE_COLUMN_NAME);
        if (null != fullTitleBB) {
            fullTitle = UTF8Type.instance.compose(fullTitleBB);
        }
        PageWritable pageWritable = new PageWritable(namespace, title,
                fullTitle, pageId, isRedirect, restrictions);
        Boolean isMinor = false;
        ByteBuffer isMinorBB = value
                .get(BenckmarkConstans.REVISION_ISMINOR_COLUMN_NAME);
        if (null != isMinorBB) {
            isMinor = BooleanType.instance.compose(value.get(

            BenckmarkConstans.REVISION_IS_MINOR));
        }
        String text = BenckmarkConstans.STRING_NULL;
        ByteBuffer textBB = value.get(BenckmarkConstans.REVISION_TEXT);
        if (null != textBB) {
            text = UTF8Type.instance.compose(textBB);
        }

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
        ContributorWritable contributorWritable = new ContributorWritable(
                contributorId, username, isAnonymous);
        String redirection = BenckmarkConstans.STRING_NULL;
        if (null != value.get(BenckmarkConstans.REVISION_REDIRECTION)) {
            redirection = UTF8Type.instance.compose(value.get(

            BenckmarkConstans.REVISION_REDIRECTION));
        }
        context.write(new Text(pageWritable.getTitle()),
                new RevisionPageCounter(title,
                        BenckmarkConstans.DATE_NULL,
                        BenckmarkConstans.INT_NULL, contributorWritable,
                        isMinor, pageWritable, text, redirection));
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
}
