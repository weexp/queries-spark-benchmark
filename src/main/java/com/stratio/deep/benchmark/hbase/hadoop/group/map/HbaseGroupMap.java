package com.stratio.deep.benchmark.hbase.hadoop.group.map;


public class HbaseGroupMap {
    // extends TableMapper<Text, RevisionWritable> {
    //
    // @Override
    // protected void map(ImmutableBytesWritable key, Result value, Context
    // context) {
    // try {
    // String uuid = Bytes.toString(key.get());
    // Integer contributorId = BenchmarkConstans.INT_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_ID)) {
    // contributorId = Bytes.toInt(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_ID));
    // }
    // String username = BenchmarkConstans.STRING_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_USER_NAME)) {
    // username = Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_USER_NAME));
    // }
    // Boolean isAnonymous = false;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_IS_ANONYMOUS)) {
    // isAnonymous = Bytes.toBoolean(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_IS_ANONYMOUS));
    // }
    // ContributorWritable contributorWritable = new ContributorWritable(
    // contributorId, username, isAnonymous);
    // Boolean isMinor = false;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.REVISION_IS_MINOR)) {
    // isMinor = Bytes.toBoolean(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.REVISION_IS_MINOR));
    // }
    // String namespace = BenchmarkConstans.STRING_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_NS)) {
    // namespace = Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_NS));
    // }
    // String title = BenchmarkConstans.STRING_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_TITLE)) {
    // title = Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_TITLE));
    // }
    // String fullTitle = BenchmarkConstans.STRING_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_FULL_TITLE)) {
    // fullTitle = Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_FULL_TITLE));
    // }
    // Integer id = BenchmarkConstans.INT_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_ID)) {
    // id = Bytes.toInt(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.CONTRIBUTOR_ID));
    // }
    // Boolean isRedirect = false;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_IS_REDIRECT)) {
    // isRedirect = Bytes.toBoolean(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_IS_REDIRECT));
    // }
    // String restrictions = BenchmarkConstans.STRING_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_RESTRICTIONS)) {
    // restrictions = Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_RESTRICTIONS));
    // }
    // PageWritable pageWritable = new PageWritable(namespace, title,
    // fullTitle, id, isRedirect, new Text(restrictions));
    //
    // String text = BenchmarkConstans.STRING_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.REVISION_TEXT)) {
    // text = Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.REVISION_TEXT));
    // }
    // String redirection = BenchmarkConstans.STRING_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.REVISION_REDIRECTION)) {
    // redirection = Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.REVISION_REDIRECTION));
    // }
    // Long ts = BenchmarkConstans.LONG_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // Bytes.toBytes(BenchmarkConstans.REVISION_TS))) {
    // ts = Bytes.toLong(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // Bytes.toBytes(BenchmarkConstans.REVISION_TS)));
    // }
    //
    // context.write(new Text(username), new RevisionWritable(
    // contributorWritable, isMinor, pageWritable, new Text(text),
    // redirection, uuid, ts));
    //
    // } catch (IOException | InterruptedException e) {
    // e.printStackTrace();
    // }
    // }
}
