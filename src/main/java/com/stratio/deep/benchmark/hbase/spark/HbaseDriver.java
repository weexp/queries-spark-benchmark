package com.stratio.deep.benchmark.hbase.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.benchmark.hbase.hadoop.HBaseDriver;
import com.stratio.deep.context.DeepSparkContext;

public class HbaseDriver {

    private static final Logger logger = LoggerFactory
            .getLogger(HBaseDriver.class);
    private static DeepSparkContext context;

    public static void main(String[] args) {

        // String sparkMaster = args[0];
        // Configuration confRevision = new Configuration();
        // String zkQuorum = args[1];
        // confRevision.set("hbase.zookeeper.quorum", zkQuorum);
        // String port = args[2];
        // confRevision.set("hbase.zookeeper.property.clientPort", port);
        // String hMaster = args[3];
        // confRevision.set("hbase.master", hMaster);
        // Configuration confPageCount = new Configuration(confRevision);
        // String revisionHTable = args[4];
        // confRevision.set(TableInputFormat.INPUT_TABLE, revisionHTable);
        // String pageCountsHTable = args[5];
        // confPageCount.set(TableInputFormat.INPUT_TABLE, pageCountsHTable);
        // SparkConf sparkConf = new SparkConf()
        // .set("spark.executor.memory", "8g");
        // sparkConf.set("spark.serializer",
        // "org.apache.spark.serializer.KryoSerializer");
        // sparkConf.set("spark.kryo.registrator",
        // "com.stratio.deep.benchmark.common.BenchMarkRegistrator");
        //
        // final List<String> slaves = Arrays.asList(args).subList(6,
        // args.length);
        // List<FileFilter> filterList = new ArrayList<>();
        //
        // final String pathFileF = args[6];
        // final String pathFileG = args[7];
        // final String pathFileJ = args[8];
        //
        // context = new CassandraDeepSparkContext(new SparkContext(sparkMaster,
        // "Benchmark", sparkConf));
        // String path = new File(HbaseDriver.class.getProtectionDomain()
        // .getCodeSource().getLocation().getPath()).getAbsolutePath();
        //
        // context.addJar(path);
        //
        // JavaPairRDD<ImmutableBytesWritable, Result> revisionRDD = context
        // .newAPIHadoopRDD(confRevision, TableInputFormat.class,
        // ImmutableBytesWritable.class, Result.class);
        //
        // JavaPairRDD<ImmutableBytesWritable, Result> pageCountsRDD = context
        // .newAPIHadoopRDD(confPageCount, TableInputFormat.class,
        // ImmutableBytesWritable.class, Result.class);
        // FileGroup fileGroup_M = new FileGroup(args[0], args[7]);
        // fileGroup_M.start();
        // for (int i = 9; i == args.length; i++) {
        // FileGroup fileGroup_S = new FileGroup(slaves.get(i), args[7]);
        // fileGroup_S.start();
        // }
        //
        // long initTime = System.currentTimeMillis();
        // JavaRDD<ResultSerializable> mapToGroupRDD = revisionRDD
        // .map(new MapRevisionFunction());
        // mapToGroupRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        // JavaPairRDD<String, Iterable<ResultSerializable>> groupByRDD =
        // mapToGroupRDD
        // .groupBy(new GroupFunction());
        // groupByRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        // JavaPairRDD<String, Integer> counts = groupByRDD
        // .mapToPair(new MapToGroupFunction());
        // long count3 = counts.count();
        // long endTime = System.currentTimeMillis();
        // logger.info("GroupBy used Hbase obtains:" + count3
        // + " registers and takes:"
        // + getMinutesFormMilis(initTime, endTime) + " minutes");
        //
        // fileGroup_M.stop();
        // for (int i = 9; i == args.length; i++) {
        // FileGroup fileGroup_S = new FileGroup(slaves.get(i), args[7]);
        // fileGroup_S.stop();
        // }
        //
        // FileJoin fileJoin_M = new FileJoin(args[0], args[8]);
        // fileJoin_M.start();
        // for (int i = 9; i == args.length; i++) {
        // FileJoin fileJoin_S = new FileJoin(slaves.get(i), args[8]);
        // fileJoin_S.start();
        // }
        //
        // initTime = System.currentTimeMillis();
        // JavaPairRDD<String, Tuple2<ResultSerializable, ResultSerializable>>
        // joinTestRDD = pageCountsRDD
        // .mapToPair(new MapPageCountFunction()).join(
        // revisionRDD.mapToPair(new MapRevisionPairFunction()));
        // long count2 = joinTestRDD.count();
        // endTime = System.currentTimeMillis();
        // logger.info("Join used Hbase with Spark obtains:" + count2
        // + " registers and takes:"
        // + getMinutesFormMilis(initTime, endTime) + " minutes");
        //
        // fileJoin_M.stop();
        // for (int i = 9; i == args.length; i++) {
        // FileJoin fileJoin_S = new FileJoin(slaves.get(i), args[8]);
        // fileJoin_S.stop();
        // }
        //
        // FileFilter fileFilter_M = new FileFilter(args[0], args[6]);
        // fileFilter_M.start();
        // for (int i = 9; i == args.length; i++) {
        // FileFilter fileFilter_S = new FileFilter(slaves.get(i), args[6]);
        // fileFilter_S.start();
        // }
        //
        // initTime = System.currentTimeMillis();
        //
        // JavaPairRDD<ImmutableBytesWritable, Result> pageRddFilter =
        // pageCountsRDD
        // .filter(new FunctionFilterSpark());
        // long count = pageRddFilter.count();
        // endTime = System.currentTimeMillis();
        //
        // logger.info("Filter used Hbase with Spark obtains:" + count
        // + " registers and takes:"
        // + getMinutesFormMilis(initTime, endTime) + " minutes");
        //
        // fileFilter_M.stop();
        // for (int i = 9; i == args.length; i++) {
        // FileFilter fileFilter_S = new FileFilter(slaves.get(i), args[6]);
        // fileFilter_S.stop();
        // }
        //
        // }
        //
        // private static Float getMinutesFormMilis(long initTime, long endTime)
        // {
        // return (Float.valueOf(endTime) - Float.valueOf(initTime)) / 1000f /
        // 60f;
    }

}
