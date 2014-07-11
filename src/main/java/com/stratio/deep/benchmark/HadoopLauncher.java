package com.stratio.deep.benchmark;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.stratio.deep.benchmark.cassandra.hadoop.CassandraDriver;

public abstract class HadoopLauncher extends Configured implements Tool {

    protected <M extends Mapper, KM extends WritableComparable, VM extends Writable, R extends Reducer, KR extends WritableComparable, VR extends Writable> void launchHadoopJob(
            String jobName, Class<M> mapperClass, String jobOuputhPath,
            Class<KM> keyToSend, Class<VM> valueToSend, Class<R> reducerClass,
            Class<KR> keyReduce, Class<VR> valueReduce, String... inputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(CassandraDriver.class);
        job.setOutputFormatClass(FileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        Path[] paths = new Path[inputPath.length];
        int i = 0;
        for (String s : inputPath) {
            paths[i] = new Path(s);
            i++;
        }
        SequenceFileInputFormat.setInputPaths(job, paths);
        job.setReducerClass(reducerClass);
        job.setMapOutputKeyClass(keyToSend);
        job.setMapOutputValueClass(valueToSend);
        job.setOutputKeyClass(keyReduce);
        job.setOutputValueClass(valueReduce);
        FileOutputFormat.setOutputPath(job, new Path(jobOuputhPath));
        job.waitForCompletion(true);

    }

    protected static Float getMinutesFormMilis(long initTime, long endTime) {
        return (Float.valueOf(endTime) - Float.valueOf(initTime)) / 1000f / 60f;
    }

}
