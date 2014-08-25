package com.stratio.deep.benchmark.hbase.spark.filter;


public class FunctionFilterSpark {
    // implements
    // Function<Tuple2<ImmutableBytesWritable, Result>, Boolean> {
    //
    // /**
    // *
    // */
    // private static final long serialVersionUID = 1137120295128520072L;
    //
    // @Override
    // public Boolean call(Tuple2<ImmutableBytesWritable, Result> v1)
    // throws Exception {
    // Result result = v1._2();
    // byte[] ts = result.getValue(BenchmarkConstans.COLUMN_FAMILY,
    // BenchmarkConstans.PAGE_COUNTER_TS);
    // if (null != ts) {
    // Calendar cal = Calendar.getInstance();
    // cal.setTimeInMillis(Bytes.toLong(ts));
    // int hour = cal.get(Calendar.HOUR_OF_DAY);
    // return hour >= 19 && hour < 20;
    // }
    // return false;
    // }
}
