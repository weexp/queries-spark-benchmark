package com.stratio.deep.benchmark.hbase.spark;


import com.stratio.deep.benchmark.cassandra.spark.FileFilter;
import com.stratio.deep.benchmark.cassandra.spark.FileGroup;
import com.stratio.deep.benchmark.cassandra.spark.FileJoin;
import com.stratio.deep.benchmark.hbase.hadoop.HBaseDriver;
import com.stratio.deep.benchmark.hbase.serialize.ResultSerializable;
import com.stratio.deep.benchmark.hbase.spark.filter.FunctionFilterSpark;
import com.stratio.deep.benchmark.hbase.spark.group.GroupFunction;
import com.stratio.deep.benchmark.hbase.spark.group.MapToGroupFunction;
import com.stratio.deep.benchmark.hbase.spark.map.MapPageCountFunction;
import com.stratio.deep.benchmark.hbase.spark.map.MapRevisionFunction;
import com.stratio.deep.benchmark.hbase.spark.map.MapRevisionPairFunction;
import com.stratio.deep.context.DeepSparkContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class HbaseDriver {

    private static final Logger logger = LoggerFactory
            .getLogger(HBaseDriver.class);
    private static DeepSparkContext context;

    public static void main(String[] args) {

        String sparkMaster = args[0];
        Configuration confRevision = new Configuration();
        String zkQuorum = args[1];
        confRevision.set("hbase.zookeeper.quorum", zkQuorum);
        String port = args[2];
        confRevision.set("hbase.zookeeper.property.clientPort", port);
        String hMaster = args[3];
        confRevision.set("hbase.master", hMaster);
        Configuration confPageCount = new Configuration(confRevision);
        String revisionHTable = args[4];
        confRevision.set(TableInputFormat.INPUT_TABLE, revisionHTable);
        String pageCountsHTable = args[5];
        confPageCount.set(TableInputFormat.INPUT_TABLE, pageCountsHTable);
        SparkConf sparkConf = new SparkConf()
                .set("spark.executor.memory", "8g");
        final String pathFileF= args[6];
        final String pathFileG= args[7];
        final String pathFileJ= args[8];


        final List<String> slaves = Arrays.asList(args).subList(9, args.length);
        List<FileFilter> filterList= new ArrayList<>();


        context = new DeepSparkContext(new SparkContext(sparkMaster,
                "Benchmark", sparkConf));
        String path = new File(HbaseDriver.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath()).getAbsolutePath();

        context.addJar(path);

        JavaPairRDD<ImmutableBytesWritable, Result> revisionRDD = context
                .newAPIHadoopRDD(confRevision, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<ImmutableBytesWritable, Result> pageCountsRDD = context
                .newAPIHadoopRDD(confPageCount, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);

        FileJoin fileJoin_M = new FileJoin(args[0], args[8]);
        fileJoin_M.start();
        for (int i=9;  i ==  args.length; i++) {
            FileJoin fileJoin_S = new FileJoin(slaves.get(i),args[8]);
            fileJoin_S.start();
        }

        long initTime = System.currentTimeMillis();
        JavaPairRDD<String, Tuple2<ResultSerializable, ResultSerializable>> joinTestRDD = pageCountsRDD
                .mapToPair(new MapPageCountFunction()).join(
                        revisionRDD.mapToPair(new MapRevisionPairFunction()));
        long count2 = joinTestRDD.count();
        long endTime = System.currentTimeMillis();
        logger.info("Join used Hbase with Spark obtains:" + count2
                + " registers and takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileJoin_M.stop();
        for (int i=9;  i ==  args.length; i++) {
            FileJoin fileJoin_S = new FileJoin(slaves.get(i),args[8]);
            fileJoin_S.stop();
        }

        FileFilter fileFilter_M = new FileFilter(args[0],args[6]);
        fileFilter_M.start();
        for (int i=9;  i ==  args.length; i++) {
            FileFilter fileFilter_S = new FileFilter(slaves.get(i),args[6]);
            fileFilter_S.start();
        }

        initTime = System.currentTimeMillis();

        JavaPairRDD<ImmutableBytesWritable, Result> pageRddFilter = pageCountsRDD
                .filter(new FunctionFilterSpark());
        long count = pageRddFilter.count();
        endTime = System.currentTimeMillis();

        logger.info("Filter used Hbase with Spark obtains:" + count
                + " registers and takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileFilter_M.stop();
        for (int i=9;  i ==  args.length; i++) {
            FileFilter fileFilter_S = new FileFilter(slaves.get(i),args[6]);
            fileFilter_S.stop();
        }

        FileGroup fileGroup_M = new FileGroup(args[0],args[7]);
        fileGroup_M.start();
        for (int i=9;  i ==  args.length; i++) {
            FileGroup fileGroup_S = new FileGroup(slaves.get(i),args[7]);
            fileGroup_S.start();
        }

        initTime = System.currentTimeMillis();
        JavaRDD<ResultSerializable> mapToGroupRDD = revisionRDD
                .map(new MapRevisionFunction());
        JavaPairRDD<String, Iterable<ResultSerializable>> groupByRDD = mapToGroupRDD
                .groupBy(new GroupFunction());
        JavaPairRDD<String, Integer> counts = groupByRDD
                .mapToPair(new MapToGroupFunction());
        long count3 = counts.count();
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Hbase takes:"

                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileGroup_M.stop();
        for (int i=9;  i ==  args.length; i++) {
            FileGroup fileGroup_S = new FileGroup(slaves.get(i),args[7]);
            fileGroup_S.stop();
        }


    }

    private static Float getMinutesFormMilis(long initTime, long endTime) {
        return (Float.valueOf(endTime) - Float.valueOf(initTime)) / 1000f / 60f;
    }

}
