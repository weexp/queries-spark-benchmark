package com.stratio.deep.benchmark.hdfs.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.benchmark.cassandra.hadoop.group.reduce.CassadraGroupReduce;
import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.HadoopLauncher;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.PageCountWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionWritable;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;
import com.stratio.deep.benchmark.hdfs.hadoop.filter.map.HDFSPageCountFilterMapper;
import com.stratio.deep.benchmark.hdfs.hadoop.filter.reduce.CassandraPageCountFilterReduce;
import com.stratio.deep.benchmark.hdfs.hadoop.group.map.GroupMapper;
import com.stratio.deep.benchmark.hdfs.hadoop.join.map.PageCountJoinMapper;
import com.stratio.deep.benchmark.hdfs.hadoop.join.map.RevisionJoinMapper;
import com.stratio.deep.benchmark.hdfs.hadoop.join.reduce.JoinReducer;
import com.stratio.deep.benchmark.hdfs.hadoop.tex.pagecounts.TextToEntityPageCountMapper;
import com.stratio.deep.benchmark.hdfs.hadoop.tex.revision.TextToEntityRevisionMapper;

public class HDFSDriver extends HadoopLauncher {

    private static final Logger logger = LoggerFactory
            .getLogger(HDFSDriver.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HDFSDriver(), args);
        System.exit(0);
    }

    private List<String> slaves;

    // String cassandraHost = "172.19.0.42";
    @Override
    public int run(String[] args) throws Exception {
        String nameNodePath = args[0];
        // final String pathFileF = args[1];
        // final String pathFileG = args[2];
        // final String pathFileJ = args[3];

        // this.slaves = Arrays.asList(args).subList(4, args.length);
        String pageCountInputPath = "/user/stratio/pageCountsText/";
        String revisionInputPath = "/user/stratio/revisionText/";

        long initTime = System.currentTimeMillis();
        /**
         * FileFilter fileFilter_M = new FileFilter(args[0], args[2]);
         * fileFilter_M.start(); for (int i = 4; i == args.length; i++) {
         * FileFilter fileFilter_S = new FileFilter(this.slaves.get(i),
         * args[2]); fileFilter_S.start(); }
         **/
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://stratiobenchmark1:8020");
        conf.addResource("/opt/hadoop/etc/hadoop/core-site.xml");
        this.setConf(conf);
        FileSystem fs = FileSystem.get(conf);

        String filterJobOuputhPath = nameNodePath + "/"
                + BenchmarkConstans.FILTER_JOB_BENCHMARK_NAME;

        String hdfsYoEntityFilter = "filterHDfsToenity";
        String filterLoadHDFSDataOutput = "loadHDFSPageCountData";
        this.launchHadoopTextJob(
                filterLoadHDFSDataOutput,
                TextToEntityPageCountMapper.class,
                hdfsYoEntityFilter,
                UUIDWritable.class,
                PageCountWritable.class,
                Reducer.class,
                UUIDWritable.class,
                PageCountWritable.class,
                new SequenceFileOutputFormat<UUIDWritable, PageCountWritable>(),
                12, this.getClass(), pageCountInputPath);

        this.launchHadoopJob(BenchmarkConstans.FILTER_JOB_BENCHMARK_NAME,
                HDFSPageCountFilterMapper.class, filterJobOuputhPath,
                PageCountWritable.class, IntWritable.class,
                CassandraPageCountFilterReduce.class, PageCountWritable.class,
                IntWritable.class,
                new TextOutputFormat<PageCountWritable, IntWritable>(), 12,
                this.getClass(), filterLoadHDFSDataOutput);

        long endTime = System.currentTimeMillis();

        logger.info("Filter used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");
        fs.delete(new Path(filterLoadHDFSDataOutput), true);
        fs.delete(new Path(filterJobOuputhPath), true);

        /**
         * fileFilter_M.stop();
         * 
         * for (int i = 5; i == args.length; i++) { FileFilter fileFilter_S1 =
         * new FileFilter(this.slaves.get(i), args[2]); fileFilter_S1.stop(); }
         * FileJoin fileJoin_M = new FileJoin(args[0], args[3]);
         * fileJoin_M.start(); for (int i = 4; i == args.length; i++) { FileJoin
         * fileJoin_S = new FileJoin(this.slaves.get(i), args[3]);
         * fileJoin_S.start(); }
         **/

        initTime = System.currentTimeMillis();

        String joinRevJobOuputhPath = nameNodePath + "/"
                + BenchmarkConstans.JOIN_JOB_BENCHMARK_NAME + "/"
                + BenchmarkConstans.TABLE_REVISION_NAME + "-"
                + BenchmarkConstans.TABLE_PAGE_COUNT_NAME;

        Job job = Job.getInstance(this.getConf(),
                BenchmarkConstans.JOIN_JOB_BENCHMARK_NAME);
        job.setJarByClass(this.getClass());
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        String pagHdfsToEntityJoin = "revGroupHDfsToenity";
        String revJLoadHDFSDataOutput = "loadHDFSJRevisionData";
        this.launchHadoopTextJob(pagHdfsToEntityJoin,
                TextToEntityRevisionMapper.class, revJLoadHDFSDataOutput,
                UUIDWritable.class, RevisionWritable.class, Reducer.class,
                UUIDWritable.class, RevisionWritable.class,
                new SequenceFileOutputFormat<UUIDWritable, RevisionWritable>(),
                12, this.getClass(), revisionInputPath);
        String hdfsYoEntityJoin = "joinHDfsToenity";
        String joinLoadPGHDFSDataOutput = "loadHDFSJPageCountData";
        this.launchHadoopTextJob(
                filterLoadHDFSDataOutput,
                TextToEntityPageCountMapper.class,
                joinLoadPGHDFSDataOutput,
                UUIDWritable.class,
                PageCountWritable.class,
                Reducer.class,
                UUIDWritable.class,
                PageCountWritable.class,
                new SequenceFileOutputFormat<UUIDWritable, PageCountWritable>(),
                12, this.getClass(), pageCountInputPath);

        MultipleInputs.addInputPath(job, new Path(joinLoadPGHDFSDataOutput),
                SequenceFileInputFormat.class, PageCountJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(revJLoadHDFSDataOutput),
                SequenceFileInputFormat.class, RevisionJoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RevisionPageCounter.class);
        job.setOutputKeyClass(RevisionPageCounter.class);
        job.setOutputValueClass(NullWritable.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(
                joinRevJobOuputhPath));
        job.setNumReduceTasks(6);
        job.waitForCompletion(true);

        endTime = System.currentTimeMillis();

        logger.info("Join used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        /**
         * fileJoin_M.stop(); for (int i = 4; i == args.length; i++) { FileJoin
         * fileJoin_S = new FileJoin(this.slaves.get(i), args[4]);
         * fileJoin_S.stop(); }
         * 
         * 
         * FileGroup fileGroup_M = new FileGroup(args[0], args[3]);
         * fileGroup_M.start(); for (int i = 5; i == args.length; i++) {
         * FileGroup fileGroup_S = new FileGroup(this.slaves.get(i), args[3]);
         * fileGroup_S.start(); }
         **/

        initTime = System.currentTimeMillis();

        String pagHdfsToEntityGroup = "revGroupHDfsToenity";
        String revGroupLoadHDFSDataOutput = "loadHDFSPageCountData";
        this.launchHadoopTextJob(revGroupLoadHDFSDataOutput,
                TextToEntityRevisionMapper.class, pagHdfsToEntityGroup,
                UUIDWritable.class, RevisionWritable.class, Reducer.class,
                UUIDWritable.class, RevisionWritable.class,
                new SequenceFileOutputFormat<UUIDWritable, RevisionWritable>(),
                12, this.getClass(), revisionInputPath);

        String groupJobOuputhPath = nameNodePath + "/"
                + BenchmarkConstans.GROUP_JOB_1_BENCHMARK_NAME;

        this.launchHadoopJob(BenchmarkConstans.GROUP_JOB_1_BENCHMARK_NAME,
                GroupMapper.class, groupJobOuputhPath,
                ContributorWritable.class, NullWritable.class,
                CassadraGroupReduce.class, ContributorWritable.class,
                IntWritable.class,
                new TextOutputFormat<ContributorWritable, IntWritable>(), 12,
                this.getClass(), revGroupLoadHDFSDataOutput);
        endTime = System.currentTimeMillis();
        logger.info("GroupBy used Cassandra takes:"
                + getMinutesFormMilis(initTime, endTime) + " minutes");

        /**
         * fileGroup_M.stop(); for (int i = 4; i == args.length; i++) {
         * FileGroup fileGroup_S = new FileGroup(this.slaves.get(i), args[3]);
         * fileGroup_S.stop(); }
         **/

        return 0;
    }
}
