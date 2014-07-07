package com.stratio.deep.benchmark.hbase.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.hbase.hadoop.filter.map.HbasePageCountMapper;
import com.stratio.deep.benchmark.hbase.hadoop.filter.reduce.HBasePageCountReduce;
import com.stratio.deep.benchmark.hbase.hadoop.group.reduce.HBaseGroup1Reduce;
import com.stratio.deep.benchmark.hbase.hadoop.group.reduce.HBaseGroup2Reduce;
import com.stratio.deep.benchmark.hbase.hadoop.join.revision.map.HbaseRevisionForJoinMapper;
import com.stratio.deep.benchmark.hbase.hadoop.join.revision.reduce.JoinReducer;
import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.PageCountWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;

public class HBaseDriver extends Configured implements Tool {

    private static final Logger logger = LoggerFactory
            .getLogger(HBaseDriver.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseDriver(), args);
        System.exit(0);
    }

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

        launchHbaseJob(config, BenckmarkConstans.JOIN_JOB_BENCHMARK_NAME,
                Arrays.asList(scanRevision, scanPageCount),
                HbaseRevisionForJoinMapper.class, joinJobOuputhPath,
                Text.class, RevisionPageCounter.class, JoinReducer.class);
        long endTime = System.currentTimeMillis();

        logger.info("Join used Hbase takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        initTime = System.currentTimeMillis();

        String filterJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.FILTER_JOB_BENCHMARK_NAME;

        launchHbaseJob(config,
                BenckmarkConstans.FILTER_JOB_BENCHMARK_NAME,
                Arrays.asList(scanPageCount), HbasePageCountMapper.class,
                filterJobOuputhPath, Text.class, PageCountWritable.class,
                HBasePageCountReduce.class);

        endTime = System.currentTimeMillis();
        logger.info("Filter used Hbase takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        initTime = System.currentTimeMillis();
        String groupJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.GROUP_JOB_1_BENCHMARK_NAME;

        launchHbaseSequenceJob(config,
                BenckmarkConstans.GROUP_JOB_1_BENCHMARK_NAME,
                Arrays.asList(scanRevision, scanPageCount),
                HbaseRevisionForJoinMapper.class, groupJobOuputhPath,
                ContributorWritable.class, IntWritable.class,
                HBaseGroup1Reduce.class);
        String group2JobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.GROUP_JOB_2_BENCHMARK_NAME;
        launchHadoopJob(config,
                BenckmarkConstans.GROUP_JOB_2_BENCHMARK_NAME,
                Mapper.class, group2JobOuputhPath, ContributorWritable.class,
                IntWritable.class, HBaseGroup2Reduce.class,
                ContributorWritable.class, IntWritable.class,
                groupJobOuputhPath);
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Hbase takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        return 0;
    }

    private static Float getMinutesFormMilis(long initTime, long endTime) {
        return (Float.valueOf(endTime) - Float.valueOf(initTime)) / 1000f / 60f;
    }

    private static <U extends TableMapper, K extends WritableComparable, V extends Writable, R extends Reducer> void launchHbaseJob(
            Configuration config, String jobName, List<Scan> scanList,
            Class<U> mapperClass, String jobOuputhPath, Class<K> keyToSend,
            Class<V> valueToSend, Class<R> reducerClass) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(HBaseDriver.class);
        TableMapReduceUtil.initTableMapperJob(scanList, mapperClass, keyToSend,
                valueToSend, job);
        job.setOutputFormatClass(FileOutputFormat.class);
        job.setReducerClass(reducerClass);
        FileOutputFormat.setOutputPath(job, new Path(jobOuputhPath));
        job.waitForCompletion(true);
    }

    private static <U extends TableMapper, K extends WritableComparable, V extends Writable, R extends Reducer> void launchHbaseSequenceJob(
            Configuration config, String jobName, List<Scan> scanList,
            Class<U> mapperClass, String jobOuputhPath, Class<K> keyToSend,
            Class<V> valueToSend, Class<R> reducerClass) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(HBaseDriver.class);
        TableMapReduceUtil.initTableMapperJob(scanList, mapperClass, keyToSend,
                valueToSend, job);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(keyToSend);
        job.setOutputValueClass(valueToSend);
        SequenceFileOutputFormat.setOutputPath(job, new Path(jobOuputhPath));
        job.waitForCompletion(true);

    }

    private static <M extends Mapper, KM extends WritableComparable, VM extends Writable, R extends Reducer, KR extends WritableComparable, VR extends Writable> void launchHadoopJob(
            Configuration config, String jobName, Class<M> mapperClass,
            String jobOuputhPath, Class<KM> keyToSend, Class<VM> valueToSend,
            Class<R> reducerClass, Class<KR> keyReduce, Class<VR> valueReduce,
            String inputPath) throws IOException, ClassNotFoundException,
            InterruptedException {
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(HBaseDriver.class);
        job.setOutputFormatClass(FileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, new Path(inputPath));
        job.setReducerClass(reducerClass);
        job.setMapOutputKeyClass(keyToSend);
        job.setMapOutputValueClass(valueToSend);
        job.setOutputKeyClass(keyReduce);
        job.setOutputValueClass(valueReduce);
        FileOutputFormat.setOutputPath(job, new Path(jobOuputhPath));
        job.waitForCompletion(true);

    }

}
