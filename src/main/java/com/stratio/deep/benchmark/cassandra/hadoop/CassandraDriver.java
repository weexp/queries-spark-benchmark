package com.stratio.deep.benchmark.cassandra.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.benchmark.BenckmarkConstans;
import com.stratio.deep.benchmark.HadoopLauncher;
import com.stratio.deep.benchmark.cassandra.hadoop.filter.map.CassandraPageCountFilterMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.filter.reduce.CassandraPageCountFilterReduce;
import com.stratio.deep.benchmark.cassandra.hadoop.group.map.GroupMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.group.reduce.CassadraGroupReduce;
import com.stratio.deep.benchmark.cassandra.hadoop.join.map.CassandraPageCountForJoinMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.join.map.CassandraRevisionForJoinMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.join.reduce.JoinReducer;
import com.stratio.deep.benchmark.cassandra.spark.FileFilter;
import com.stratio.deep.benchmark.cassandra.spark.FileGroup;
import com.stratio.deep.benchmark.cassandra.spark.FileJoin;
import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;

public class CassandraDriver extends HadoopLauncher {

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

    // String cassandraHost = "172.19.0.42";
    @Override
    public int run(String[] args) throws Exception {
        String nameNodePath = args[0];
        this.CASSANDRAHOST = args[1];
        this.slaves = Arrays.asList(args).subList(2, args.length);
        List<FileFilter> filterList = new ArrayList<>();
        // initialization files Master and Slaves for Filter
        /*
         * for (String node : slaves) { FileFilter fileFilterF_M = new
         * FileFilter(node); fileFilterF_M.start();
         * filterList.add(fileFilterF_M); } for (FileFilter filter : filterList)
         * { filter.stop(); } FileFilter fileFilterF_M = new
         * FileFilter(args[1]); fileFilterF_M.start();
         */

        FileJoin fileJoin_M = new FileJoin(args[0]);
        fileJoin_M.start();
        for (int i = 0; i == args.length; i++) {
            FileJoin fileJoin_S1 = new FileJoin(this.slaves.get(i));
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
        this.launchHadoopJob(BenckmarkConstans.JOIN_JOB_BENCHMARK_NAME,
                Mapper.class, BenckmarkConstans.JOIN_JOB_BENCHMARK_NAME
                        + "/JOIN/", Text.class, RevisionPageCounter.class,
                JoinReducer.class, ContributorWritable.class,
                NullWritable.class, joinRevJobOuputhPath, joinPGJobOuputhPath);

        long endTime = System.currentTimeMillis();

        logger.info("Join used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileJoin_M.stop();
        // fileJoin_S1.stop();
        for (int i = 0; i == args.length; i++) {
            FileJoin fileJoin_S1 = new FileJoin(this.slaves.get(i));
            fileJoin_S1.stop();
        }

        initTime = System.currentTimeMillis();

        FileFilter fileFilterF_M = new FileFilter(args[0]);
        fileFilterF_M.start();
        for (int i = 0; i == args.length; i++) {
            FileFilter fileFilter_S1 = new FileFilter(this.slaves.get(i));
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

        for (int i = 0; i == args.length; i++) {
            FileFilter fileFilter_S1 = new FileFilter(this.slaves.get(i));
            fileFilter_S1.stop();
        }

        FileGroup fileGroup_M = new FileGroup(args[0]);
        fileGroup_M.start();
        for (int i = 0; i == args.length; i++) {
            FileGroup fileGroup_S1 = new FileGroup(this.slaves.get(i));
            fileGroup_S1.start();
        }

        initTime = System.currentTimeMillis();
        String groupJobOuputhPath = nameNodePath + "/"
                + BenckmarkConstans.GROUP_JOB_1_BENCHMARK_NAME;

        this.launchHadoopJob(BenckmarkConstans.GROUP_JOB_1_BENCHMARK_NAME,
                GroupMapper.class, groupJobOuputhPath,
                ContributorWritable.class, NullWritable.class,
                CassadraGroupReduce.class, ContributorWritable.class,
                IntWritable.class, groupJobOuputhPath);
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileGroup_M.stop();
        for (int i = 0; i == args.length; i++) {
            FileGroup fileGroup_S1 = new FileGroup(this.slaves.get(i));
            fileGroup_S1.stop();
        }

        return 0;
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

}
