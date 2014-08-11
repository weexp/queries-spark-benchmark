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

import com.stratio.deep.benchmark.cassandra.hadoop.filter.map.CassandraPageCountFilterMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.filter.reduce.CassandraPageCountFilterReduce;
import com.stratio.deep.benchmark.cassandra.hadoop.group.map.GroupMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.group.reduce.CassadraGroupReduce;
import com.stratio.deep.benchmark.cassandra.hadoop.join.map.CassandraPageCountForJoinMapper;
import com.stratio.deep.benchmark.cassandra.hadoop.join.map.CassandraRevisionForJoinMapper;
import com.stratio.deep.benchmark.cassandra.spark.FileFilter;
import com.stratio.deep.benchmark.cassandra.spark.FileGroup;
import com.stratio.deep.benchmark.cassandra.spark.FileJoin;
import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.HadoopLauncher;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;

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
        // initialization files Master and Slaves for Filter
        /*
         * for (String node : slaves) { FileFilter fileFilterF_M = new
         * FileFilter(node); fileFilterF_M.start();
         * filterList.add(fileFilterF_M); } for (FileFilter filter : filterList)
         * { filter.stop(); } FileFilter fileFilterF_M = new
         * FileFilter(args[1]); fileFilterF_M.start();
         */
        this.CASSANDRAHOST = args[1];
        final String pathFileF = args[2];
        final String pathFileG = args[3];
        final String pathFileJ = args[4];

        this.slaves = Arrays.asList(args).subList(5, args.length);
        List<FileFilter> filterList = new ArrayList<>();

        FileJoin fileJoin_M = new FileJoin(args[0], args[4]);
        fileJoin_M.start();
        for (int i = 5; i == args.length; i++) {
            FileJoin fileJoin_S = new FileJoin(this.slaves.get(i), args[4]);
            fileJoin_S.start();
        }

        this.setConf(new Configuration());
        long initTime = System.currentTimeMillis();

        String joinRevJobOuputhPath = nameNodePath + "/"
                + BenchmarkConstans.JOIN_JOB_BENCHMARK_NAME + "/"
                + BenchmarkConstans.TABLE_REVISION_NAME;
        String joinPGJobOuputhPath = nameNodePath + "/"
                + BenchmarkConstans.JOIN_JOB_BENCHMARK_NAME + "/"
                + BenchmarkConstans.TABLE_PAGE_COUNT_NAME;

        this.launchCassandraJob(BenchmarkConstans.TABLE_REVISION_NAME,
                new Path(joinRevJobOuputhPath),
                CassandraRevisionForJoinMapper.class,
                BenchmarkConstans.NUM_PAG, Reducer.class, Text.class,
                RevisionPageCounter.class, Text.class,
                RevisionPageCounter.class);

        this.launchCassandraJob(BenchmarkConstans.TABLE_PAGE_COUNT_NAME,
                new Path(joinPGJobOuputhPath),
                CassandraPageCountForJoinMapper.class,
                BenchmarkConstans.NUM_PAG, Reducer.class, Text.class,
                RevisionPageCounter.class, Text.class,
                RevisionPageCounter.class);
        // this.launchHadoopJob(BenchmarkConstans.JOIN_JOB_BENCHMARK_NAME,
        // Mapper.class, BenchmarkConstans.JOIN_JOB_BENCHMARK_NAME
        // + "/JOIN/", Text.class, RevisionPageCounter.class,
        // JoinReducer.class, ContributorWritable.class,
        // NullWritable.class, joinRevJobOuputhPath, joinPGJobOuputhPath);

        long endTime = System.currentTimeMillis();

        logger.info("Join used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileJoin_M.stop();
        for (int i = 5; i == args.length; i++) {
            FileJoin fileJoin_S = new FileJoin(this.slaves.get(i), args[4]);
            fileJoin_S.stop();
        }

        initTime = System.currentTimeMillis();

        FileFilter fileFilter_M = new FileFilter(args[0], args[2]);
        fileFilter_M.start();
        for (int i = 5; i == args.length; i++) {
            FileFilter fileFilter_S = new FileFilter(this.slaves.get(i),
                    args[2]);
            fileFilter_S.start();
        }

        String filterJobOuputhPath = nameNodePath + "/"
                + BenchmarkConstans.FILTER_JOB_BENCHMARK_NAME;

        this.launchCassandraJob(BenchmarkConstans.TABLE_PAGE_COUNT_NAME,
                new Path(filterJobOuputhPath),
                CassandraPageCountFilterMapper.class,
                BenchmarkConstans.NUM_PAG,
                CassandraPageCountFilterReduce.class, IntWritable.class,
                NullWritable.class, IntWritable.class, NullWritable.class);

        endTime = System.currentTimeMillis();
        logger.info("Filter used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileFilter_M.stop();

        for (int i = 5; i == args.length; i++) {
            FileFilter fileFilter_S1 = new FileFilter(this.slaves.get(i),
                    args[2]);
            fileFilter_S1.stop();
        }

        FileGroup fileGroup_M = new FileGroup(args[0], args[3]);
        fileGroup_M.start();
        for (int i = 5; i == args.length; i++) {
            FileGroup fileGroup_S = new FileGroup(this.slaves.get(i), args[3]);
            fileGroup_S.start();
        }

        initTime = System.currentTimeMillis();
        String groupJobOuputhPath = nameNodePath + "/"
                + BenchmarkConstans.GROUP_JOB_1_BENCHMARK_NAME;

        this.launchHadoopJob(BenchmarkConstans.GROUP_JOB_1_BENCHMARK_NAME,
                GroupMapper.class, groupJobOuputhPath,
                ContributorWritable.class, NullWritable.class,
                CassadraGroupReduce.class, ContributorWritable.class,
                IntWritable.class, null, 12, this.getClass(), "");
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        fileGroup_M.stop();
        for (int i = 5; i == args.length; i++) {
            FileGroup fileGroup_S = new FileGroup(this.slaves.get(i), args[3]);
            fileGroup_S.stop();
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
