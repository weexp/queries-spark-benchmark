package com.stratio.deep.benchmark;

import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

import com.stratio.deep.benchmark.hbase.serialize.DataType;
import com.stratio.deep.benchmark.hbase.serialize.HColumnFamilyMetadata;
import com.stratio.deep.benchmark.hbase.serialize.HQualifiersMetadata;
import com.stratio.deep.benchmark.hbase.serialize.HTableMetadata;

public class BenckmarkConstans {

    public static final String COLUMN_FAMILY_NAME = "a";

    public static final String REVISION_TEXT_COLUMN_NAME = "revision_text";
    public static final String REVISION_REDIRECTION_COLUMN_NAME = "revision_redirection";
    public static final String REVISION_ISMINOR_COLUMN_NAME = "revision_isminor";
    public static final String REVISION_ID_COLUMN_NAME = "revision_id";
    public static final String ID_COLUMN_NAME = "id";
    public static final String CONTRIBUTOR_ISANONYMOUS_COLUMN_NAME = "contributor_isanonymous";
    public static final String CONTRIBUTOR_USERNAME_COLUMN_NAME = "contributor_username";
    public static final String CONTRIBUTOR_ID_COLUMN_NAME = "contributor_id";
    public static final String PAGE_ISREDIRECT_COLUMN_NAME = "page_isredirect";
    public static final String PAGE_RESTRICTIONS_COLUMN_NAME = "page_restrictions";
    public static final String PAGE_FULL_TITLE_COLUMN_NAME = "page_fulltitle";

    public static final String PAGE_COUNTER_TITLE_COLUMN_NAME = "title";
    public static final String PAGE_COUNTS_PAGECOUNTS_COLUMN_NAME = "pagecounts";
    public static final String PAGE_COUNTS_TS_COLUMN_NAME = "ts";

    public static final byte[] TOKENS_COLUMN_FAMILY = Bytes.toBytes("t");
    public static final byte[] LTOKENS_COLUMN_FAMILY = Bytes.toBytes("lt");
    public static final byte[] PAGE_RESTRICTIONS = Bytes
            .toBytes(PAGE_RESTRICTIONS_COLUMN_NAME);
    public static final byte[] PAGE_IS_REDIRECT = Bytes
            .toBytes(PAGE_ISREDIRECT_COLUMN_NAME);
    public static final byte[] CONTRIBUTOR_ID = Bytes
            .toBytes(CONTRIBUTOR_ID_COLUMN_NAME);
    public static final byte[] CONTRIBUTOR_USER_NAME = Bytes
            .toBytes(CONTRIBUTOR_USERNAME_COLUMN_NAME);
    public static final byte[] CONTRIBUTOR_IS_ANONYMOUS = Bytes
            .toBytes(CONTRIBUTOR_ISANONYMOUS_COLUMN_NAME);
    public static final byte[] ID = Bytes.toBytes(ID_COLUMN_NAME);
    public static final byte[] REVISION_ID = Bytes
            .toBytes(REVISION_ID_COLUMN_NAME);
    public static final byte[] REVISION_IS_MINOR = Bytes
            .toBytes(REVISION_ISMINOR_COLUMN_NAME);
    public static final byte[] REVISION_REDIRECTION = Bytes
            .toBytes(REVISION_REDIRECTION_COLUMN_NAME);
    public static final byte[] REVISION_TEXT = Bytes
            .toBytes(REVISION_TEXT_COLUMN_NAME);

    public static final byte[] PAGE_COUNTER_TITLE = Bytes
            .toBytes(PAGE_COUNTER_TITLE_COLUMN_NAME);
    public static final byte[] PAGE_COUNTER_TS = Bytes
            .toBytes(PAGE_COUNTS_TS_COLUMN_NAME);
    public static final byte[] PAGE_COUNTER_COUNT = Bytes
            .toBytes(PAGE_COUNTS_PAGECOUNTS_COLUMN_NAME);
    public static final Integer INT_NULL = -1;
    public static final Long LONG_NULL = -1l;
    public static final String STRING_NULL = "";
    public static final Date DATE_NULL = new Date(0);
    public static final Boolean BOOLEAN_NULL = false;
    public static final String[] ARRAY_NULL = new String[] { "" };
    public static final String TABLE_REVISION_NAME = "revision";
    public static final String TABLE_PAGE_COUNT_NAME = "page_counter";
    public static final String JOIN_JOB_BENCHMARK_NAME = "JoinJobBenchMark";
    public static final String FILTER_JOB_BENCHMARK_NAME = "filterJobBenchMark";
    public static final HTableMetadata PAGE_COUNT_METADATA = new HTableMetadata(
            "pagecounts", new HColumnFamilyMetadata(COLUMN_FAMILY_NAME,
                    new HQualifiersMetadata(ID_COLUMN_NAME, DataType.String),
                    new HQualifiersMetadata(PAGE_COUNTER_TITLE_COLUMN_NAME,
                            DataType.String),
                    new HQualifiersMetadata(PAGE_COUNTS_PAGECOUNTS_COLUMN_NAME,
                            DataType.String), new HQualifiersMetadata(
                            PAGE_COUNTS_PAGECOUNTS_COLUMN_NAME, DataType.Date)));
    public static final String PAGE_ID_COLUMN_NAME = "page_id";
    public static final String PAGE_NS_COLUMN_NAME = "page_ns";
    public static final String PAGE_TITLE_COLUMN_NAME = "page_title";

    public static final HTableMetadata REVISION_METADATA = new HTableMetadata(
            "revision", new HColumnFamilyMetadata(COLUMN_FAMILY_NAME,
                    new HQualifiersMetadata(ID_COLUMN_NAME, DataType.String),
                    new HQualifiersMetadata(REVISION_TEXT_COLUMN_NAME,
                            DataType.String), new HQualifiersMetadata(
                            REVISION_REDIRECTION_COLUMN_NAME, DataType.String),
                    new HQualifiersMetadata(REVISION_ISMINOR_COLUMN_NAME,
                            DataType.Boolean), new HQualifiersMetadata(
                            REVISION_ID_COLUMN_NAME, DataType.Integer),
                    new HQualifiersMetadata(ID_COLUMN_NAME, DataType.String),
                    new HQualifiersMetadata(
                            CONTRIBUTOR_ISANONYMOUS_COLUMN_NAME,
                            DataType.Boolean), new HQualifiersMetadata(
                            CONTRIBUTOR_USERNAME_COLUMN_NAME, DataType.String),
                    new HQualifiersMetadata(CONTRIBUTOR_ID_COLUMN_NAME,
                            DataType.Integer), new HQualifiersMetadata(
                            PAGE_ISREDIRECT_COLUMN_NAME, DataType.Boolean),
                    new HQualifiersMetadata(PAGE_RESTRICTIONS_COLUMN_NAME,
                            DataType.String), new HQualifiersMetadata(
                            PAGE_FULL_TITLE_COLUMN_NAME, DataType.String),
                    new HQualifiersMetadata(PAGE_ID_COLUMN_NAME,
                            DataType.Integer), new HQualifiersMetadata(
                            PAGE_NS_COLUMN_NAME, DataType.String),
                    new HQualifiersMetadata(PAGE_TITLE_COLUMN_NAME,
                            DataType.String)));
    public static final String PAGE_COUNT_TS_COLUMN_NAME = PAGE_COUNTS_TS_COLUMN_NAME;
    public static final byte[] PAGE_COUNT_TS = Bytes
            .toBytes(PAGE_COUNT_TS_COLUMN_NAME);
    public static final String GROUP_JOB_1_BENCHMARK_NAME = "groupJobBenchMark1";
    public static final String GROUP_JOB_2_BENCHMARK_NAME = "groupJobBenchMark2";
    public static final byte[] PAGE_TITLE = Bytes
            .toBytes(PAGE_TITLE_COLUMN_NAME);
    public static final byte[] PAGE_ID = Bytes.toBytes(PAGE_ID_COLUMN_NAME);
    public static final byte[] PAGE_NS = Bytes.toBytes(PAGE_NS_COLUMN_NAME);
    public static final byte[] PAGE_FULL_TITLE = Bytes
            .toBytes(PAGE_FULL_TITLE_COLUMN_NAME);
    public static final String NUM_PAG = "1000";

    public static final byte[] COLUMN_FAMILY = Bytes
            .toBytes(COLUMN_FAMILY_NAME);

}
