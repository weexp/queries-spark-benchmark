package com.stratio.deep.benchmark.hbase.hadoop.filter.map;


public class HbasePageCountMapper {
    // extends
    // TableMapper<PageCountWritable, NullWritable> {
    //
    // @Override
    // protected void setup(Context context) throws IOException,
    // InterruptedException {
    // super.setup(context);
    // }
    //
    // @Override
    // protected void map(ImmutableBytesWritable key, Result value, Context
    // context)
    // throws IOException, InterruptedException {
    //
    // Long ts = BenchmarkConstans.LONG_NULL;
    // if (null != value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_COUNT_TS)) {
    // ts = Bytes.toLong(value.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_COUNT_TS));
    // }
    // Calendar cal = Calendar.getInstance();
    // cal.setTimeInMillis(ts);
    // int hour = cal.get(Calendar.HOUR_OF_DAY);
    // if (hour >= 19 && hour < 20) {
    // context.write(
    // new PageCountWritable(ts, Bytes.toString(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_COUNTER_TITLE)), Bytes
    // .toInt(value.getValue(
    // BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_COUNTER_COUNT))),
    // NullWritable.get());
    // }
    // }
}
