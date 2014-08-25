package com.stratio.deep.benchmark.hbase.spark.map;


public class MapRevisionPairFunction {
    // implements
    // PairFunction<Tuple2<ImmutableBytesWritable, Result>, String,
    // ResultSerializable> {
    //
    // /**
    // *
    // */
    // private static final long serialVersionUID = -7467004678412540149L;
    //
    // @Override
    // public Tuple2<String, ResultSerializable> call(
    // Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
    // ResultSerializable result = this.convertPageConterResulToSerializable(t
    // ._2());
    // String title = (String) result.getValue(
    // BenchmarkConstans.COLUMN_FAMILY_NAME,
    // BenchmarkConstans.PAGE_TITLE_COLUMN_NAME);
    // return new Tuple2<String, ResultSerializable>(title, result);
    // }
    //
    // private ResultSerializable convertPageConterResulToSerializable(
    // Result result) {
    // return ResultSerializable.builder(result,
    // BenchmarkConstans.REVISION_METADATA);
    // }
}
