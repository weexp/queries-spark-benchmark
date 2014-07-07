package com.stratio.deep.benchmark.hbase.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.stratio.deep.benchmark.hbase.hadoop.HBaseDriver;
import com.stratio.deep.benchmark.hbase.spark.filter.FunctionFilterSpark;
import com.stratio.deep.benchmark.hbase.spark.group.GroupFunction;
import com.stratio.deep.benchmark.hbase.spark.map.MapPageCountFunction;
import com.stratio.deep.benchmark.hbase.spark.map.MapRevisionFunction;
import com.stratio.deep.context.DeepSparkContext;

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

        context = new DeepSparkContext(new SparkContext(sparkMaster,
                "Hbase com.stratio.deep.benchmark", sparkConf));

        JavaPairRDD<ImmutableBytesWritable, Result> revisionRDD = context
                .newAPIHadoopRDD(confRevision, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<ImmutableBytesWritable, Result> pageCountsRDD = context
                .newAPIHadoopRDD(confPageCount, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        long initTime = System.currentTimeMillis();
        JavaPairRDD<ImmutableBytesWritable, Result> pageRddFilter = pageCountsRDD
                .filter(new FunctionFilterSpark());
        long count = pageRddFilter.count();
        long endTime = System.currentTimeMillis();
        logger.info("Filter used Hbase with Spark obtains:" + count
                + " registers and takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        initTime = System.currentTimeMillis();
        JavaPairRDD<String, Tuple2<Result, Result>> join = pageCountsRDD
                .mapToPair(new MapPageCountFunction()).join(
                        revisionRDD.mapToPair(new MapRevisionFunction()));
        JavaPairRDD<String, Iterable<Tuple2<String, Tuple2<Result, Result>>>> groupByRDD = join
                .groupBy(new GroupFunction());
        groupByRDD.count();
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Hbase takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        initTime = System.currentTimeMillis();
        JavaPairRDD<String, Tuple2<Result, Result>> joinTestRDD = pageCountsRDD
                .mapToPair(new MapPageCountFunction()).join(
                        revisionRDD.mapToPair(new MapRevisionFunction()));
        long count2 = joinTestRDD.count();
        endTime = System.currentTimeMillis();
        logger.info("Join used Hbase with Spark obtains:" + count2
                + " registers and takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

    }

    private static Float getMinutesFormMilis(long initTime, long endTime) {
        return (Float.valueOf(endTime) - Float.valueOf(initTime)) / 1000f / 60f;
    }

}
