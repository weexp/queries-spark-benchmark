package com.stratio.deep.benchmark.cassandra.hadoop;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.cassandra.hadoop.filter.map.CassandraPageCountFilterMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.filter.reduce.CassandraPageCountFilterReduce;
import com.stratio.deep.benchmark.cassandra.hadoop.group.reduce.HBaseGroup2Reduce;
import com.stratio.deep.benchmark.cassandra.hadoop.join.map.CassandraPageCountForJoinMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.join.map.CassandraRevisionForJoinMapper;
import com.stratio.deep.benchmark.cassandra.spark.FileFilter;
import com.stratio.deep.benchmark.cassandra.spark.FileGroup;
import com.stratio.deep.benchmark.cassandra.spark.FileJoin;
import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CassandraDriver extends Configured implements Tool {

    private static final Logger logger = LoggerFactory
            .getLogger(CassandraDriver.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CassandraDriver(), args);
        System.exit(0);
    }

    private String CASSANDRA_PORT;
    private String CASSANDRAHOST;
    private String KEYSPACE;
    private String PARTTIONER;
    private List<String> slaves;

    //String cassandraHost = "172.19.0.42";
    @Override
    public int run(String[] args) throws Exception {
        String nameNodePath = args[0];
        CASSANDRAHOST = args[1];
        slaves = Arrays.asList(args).subList(2, args.length);
        List<FileFilter> filterList= new ArrayList<>();
        // initialization files Master and Slaves for Filter
        /*for (String node : slaves)
        {
            FileFilter fileFilterF_M = new FileFilter(node);
            fileFilterF_M.start();
            filterList.add(fileFilterF_M);
        }
        for (FileFilter filter : filterList)
        {
           filter.stop();
        }
        FileFilter fileFilterF_M = new FileFilter(args[1]);
        fileFilterF_M.start();
        */

        FileJoin fileJoin_M = new FileJoin(args[0]);
        fileJoin_M.start();
        for (int i=0;  i ==  args.length; i++) {
            FileJoin fileJoin_S1 = new FileJoin(slaves.get(i));
            fileJoin_S1.start();
        }

        this.setConf(new Configuration());
        long initTime = System.currentTimeMillis();

        String joinRevJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.JOIN_JOB_BENCHMARK_NAME + "/"
                + BenckmarkConstans.TABLE_REVISION_NAME;
        String joinPGJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.JOIN_JOB_BENCHMARK_NAME + "/"
                + BenckmarkConstans.TABLE_PAGE_COUNT_NAME;

        this.launchCassandraJob(BenckmarkConstans.TABLE_REVISION_NAME,
                new Path(joinRevJobOuputhPath),
                CassandraRevisionForJoinMapper.class,
                BenckmarkConstans.NUM_PAG, Reducer.class, Text.class,
                RevisionPageCounter.class, Text.class,
                RevisionPageCounter.class);

        this.launchCassandraJob(BenckmarkConstans.TABLE_PAGE_COUNT_NAME,
                new Path(joinPGJobOuputhPath),
                CassandraPageCountForJoinMapper.class,
                BenckmarkConstans.NUM_PAG, Reducer.class, Text.class,
                RevisionPageCounter.class, Text.class,
                RevisionPageCounter.class);

        long endTime = System.currentTimeMillis();

        logger.info("Join used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileJoin_M.stop();
        //fileJoin_S1.stop();
        for (int i=0;  i ==  args.length; i++) {
            FileJoin fileJoin_S1 = new FileJoin(slaves.get(i));
            fileJoin_S1.stop();
        }

        initTime = System.currentTimeMillis();

        FileFilter fileFilterF_M = new FileFilter(args[0]);
        fileFilterF_M.start();
        for (int i=0;  i ==  args.length; i++) {
            FileFilter fileFilter_S1 = new FileFilter(slaves.get(i));
            fileFilter_S1.start();
        }

        String filterJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.FILTER_JOB_BENCHMARK_NAME;

        this.launchCassandraJob(BenckmarkConstans.TABLE_PAGE_COUNT_NAME,
                new Path(filterJobOuputhPath),
                CassandraPageCountFilterMapper.class,
                BenckmarkConstans.NUM_PAG,
                CassandraPageCountFilterReduce.class, IntWritable.class,
                NullWritable.class, IntWritable.class, NullWritable.class);

        endTime = System.currentTimeMillis();
        logger.info("Filter used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileFilterF_M.stop();

        for (int i=0;  i ==  args.length; i++) {
            FileFilter fileFilter_S1 = new FileFilter(slaves.get(i));
            fileFilter_S1.stop();
        }

        FileGroup fileGroup_M = new FileGroup(args[0]);
        fileGroup_M.start();
        for (int i=0;  i ==  args.length; i++) {
            FileGroup fileGroup_S1 = new FileGroup(slaves.get(i));
            fileGroup_S1.start();
        }

        initTime = System.currentTimeMillis();
        String groupJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.GROUP_JOB_1_BENCHMARK_NAME;
        /**
         * 
         * TODO: Do again the join between PageCount and Revision
         * 
         */
        String group2JobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.GROUP_JOB_2_BENCHMARK_NAME;

        this.launchHadoopJob(BenckmarkConstans.GROUP_JOB_2_BENCHMARK_NAME,
                Mapper.class, group2JobOuputhPath, ContributorWritable.class,
                IntWritable.class, HBaseGroup2Reduce.class,
                ContributorWritable.class, IntWritable.class,
                groupJobOuputhPath);
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileGroup_M.stop();
        for (int i=0;  i ==  args.length; i++) {
            FileGroup fileGroup_S1 = new FileGroup(slaves.get(i));
            fileGroup_S1.stop();
        }

        return 0;
    }

    private static Float getMinutesFormMilis(long initTime, long endTime) {
        return (Float.valueOf(endTime) - Float.valueOf(initTime)) / 1000f / 60f;
    }

    private <T extends Mapper, U extends Reducer, W extends WritableComparable, Z extends Writable, X extends WritableComparable, Y extends Writable> void launchCassandraJob(
            String inputTable, Path outPuthDir, Class<T> mapperClass,
            String numPag, Class<U> redduceClass, Class<W> mapOuputKeyClass,
            Class<Z> mapOuputValueClass, Class<X> ouputKeyClass,
            Class<Y> ouputValueClass) throws ClassNotFoundException,
            IOException, InterruptedException {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(this.getClass());
        job.setMapperClass(mapperClass);
        job.setReducerClass(redduceClass);

        job.setMapOutputKeyClass(mapOuputKeyClass);
        job.setMapOutputValueClass(mapOuputValueClass);
        job.setOutputKeyClass(ouputKeyClass);
        job.setOutputValueClass(ouputValueClass);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outPuthDir);
        job.getConfiguration();
        job.setInputFormatClass(CqlPagingInputFormat.class);
        ConfigHelper.setInputRpcPort(job.getConfiguration(),
                this.CASSANDRA_PORT);
        ConfigHelper.setInputInitialAddress(job.getConfiguration(),
                this.CASSANDRAHOST);
        ConfigHelper.setInputColumnFamily(job.getConfiguration(),
                this.KEYSPACE, inputTable);
        ConfigHelper.setInputPartitioner(job.getConfiguration(),
                this.PARTTIONER);
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), numPag);
        job.setNumReduceTasks(6);
        job.waitForCompletion(true);

    }

    private <M extends Mapper, KM extends WritableComparable, VM extends Writable, R extends Reducer, KR extends WritableComparable, VR extends Writable> void launchHadoopJob(
            String jobName, Class<M> mapperClass, String jobOuputhPath,
            Class<KM> keyToSend, Class<VM> valueToSend, Class<R> reducerClass,
            Class<KR> keyReduce, Class<VR> valueReduce, String inputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(CassandraDriver.class);
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
