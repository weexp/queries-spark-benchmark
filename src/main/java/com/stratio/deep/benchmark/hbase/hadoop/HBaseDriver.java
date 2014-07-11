package com.stratio.deep.benchmark.hbase.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.HadoopLauncher;
import com.stratio.deep.benchmark.hbase.hadoop.filter.map.HbasePageCountMapper;
import com.stratio.deep.benchmark.hbase.hadoop.filter.reduce.HBasePageCountReduce;
import com.stratio.deep.benchmark.hbase.hadoop.group.reduce.HBaseGroupReduce;
import com.stratio.deep.benchmark.hbase.hadoop.join.map.HbaseRevisionForJoinMapper;
import com.stratio.deep.benchmark.hbase.hadoop.join.reduce.JoinReducer;
import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.PageCountWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;

public class HBaseDriver extends HadoopLauncher {

    private static final Logger logger = LoggerFactory
            .getLogger(HBaseDriver.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseDriver(), args);
        System.exit(0);
    }

    @Override
    public int run(String[] args) throws Exception {
        String nameNodePath = args[0];

        Configuration config = HBaseConfiguration.create();
        Scan scanRevision = new Scan();
        scanRevision.setCaching(500);
        scanRevision.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes(BenckmarkConstans.TABLE_REVISION_NAME));
        Scan scanPageCount = new Scan();
        scanPageCount.setCaching(500);
        scanPageCount.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes(BenckmarkConstans.TABLE_PAGE_COUNT_NAME));

        long initTime = System.currentTimeMillis();

        String joinJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.JOIN_JOB_BENCHMARK_NAME;

        launchHbaseJob(
                config,
                BenckmarkConstans.JOIN_JOB_BENCHMARK_NAME,
                Arrays.asList(scanRevision, scanPageCount),
                HbaseRevisionForJoinMapper.class,
                joinJobOuputhPath,
                Text.class,
                RevisionPageCounter.class,
                JoinReducer.class,
                RevisionPageCounter.class,
                IntWritable.class,
                new SequenceFileOutputFormat<RevisionPageCounter, IntWritable>());
        long endTime = System.currentTimeMillis();

        logger.info("Join used Hbase takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        initTime = System.currentTimeMillis();

        String filterJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.FILTER_JOB_BENCHMARK_NAME;

        launchHbaseJob(config, BenckmarkConstans.FILTER_JOB_BENCHMARK_NAME,
                Arrays.asList(scanPageCount), HbasePageCountMapper.class,
                filterJobOuputhPath, Text.class, PageCountWritable.class,
                HBasePageCountReduce.class, IntWritable.class,
                NullWritable.class,
                new SequenceFileOutputFormat<IntWritable, NullWritable>());

        endTime = System.currentTimeMillis();
        logger.info("Filter used Hbase takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        initTime = System.currentTimeMillis();
        String groupJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.GROUP_JOB_1_BENCHMARK_NAME;

        launchHbaseJob(
                config,
                BenckmarkConstans.GROUP_JOB_1_BENCHMARK_NAME,
                Arrays.asList(scanRevision, scanPageCount),
                HbaseRevisionForJoinMapper.class,
                groupJobOuputhPath,
                ContributorWritable.class,
                IntWritable.class,
                HBaseGroupReduce.class,
                ContributorWritable.class,
                IntWritable.class,
                new SequenceFileOutputFormat<ContributorWritable, IntWritable>());
        String group2JobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.GROUP_JOB_2_BENCHMARK_NAME;
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Hbase takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        return 0;
    }

    private static <U extends TableMapper, K extends WritableComparable, V extends Writable, R extends Reducer, KR extends WritableComparable, VR extends Writable, O extends FileOutputFormat> void launchHbaseJob(
            Configuration config, String jobName, List<Scan> scanList,
            Class<U> mapperClass, String jobOuputhPath, Class<K> keyToSend,
            Class<V> valueToSend, Class<R> reducerClass, Class<KR> reduceKey,
            Class<VR> reduceValue, O outputFormat) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(HBaseDriver.class);
        TableMapReduceUtil.initTableMapperJob(scanList, mapperClass, keyToSend,
                valueToSend, job);
        job.setOutputFormatClass(outputFormat.getClass());
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(reduceKey);
        job.setOutputValueClass(reduceValue);
        O.setOutputPath(job, new Path(jobOuputhPath));
        job.waitForCompletion(true);
    }

}
