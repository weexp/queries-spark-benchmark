package com.stratio.deep.benchmark.common;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.stratio.deep.benchmark.common.hadoop.counter.map.CounterMap;
import com.stratio.deep.benchmark.common.hadoop.counter.reduce.CounterReducer;

public abstract class HadoopLauncher extends Configured implements Tool {

    @SuppressWarnings({ "rawtypes" })
    protected <M extends Mapper, KM extends WritableComparable, VM extends Writable, R extends Reducer, KR extends WritableComparable, VR extends Writable, O extends FileOutputFormat<KR, VR>> void launchHadoopJob(
            String jobName, Class<M> mapperClass, String jobOuputhPath,
            Class<KM> keyToSend, Class<VM> valueToSend, Class<R> reducerClass,
            Class<KR> keyReduce, Class<VR> valueReduce, O outputFormat,
            Integer numReduce, Class mainClass, String... inputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(mainClass);
        job.setOutputFormatClass(outputFormat.getClass());
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
        O.setOutputPath(job, new Path(jobOuputhPath));
        job.setNumReduceTasks(numReduce);
        job.waitForCompletion(true);
    }

    @SuppressWarnings({ "rawtypes" })
    protected <M extends Mapper, KM extends WritableComparable, VM extends Writable, R extends Reducer, KR extends WritableComparable, VR extends Writable, O extends FileOutputFormat<KR, VR>> void launchHadoopTextJob(
            String jobName, Class<M> mapperClass, String jobOuputhPath,
            Class<KM> keyToSend, Class<VM> valueToSend, Class<R> reducerClass,
            Class<KR> keyReduce, Class<VR> valueReduce, O outputFormat,
            Integer numReduce, Class mainClass, String... inputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(mainClass);
        job.setOutputFormatClass(outputFormat.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        Path[] paths = new Path[inputPath.length];
        int i = 0;
        for (String s : inputPath) {
            paths[i] = new Path(s);
            i++;
        }
        TextInputFormat.setInputPaths(job, paths);
        job.setReducerClass(reducerClass);
        // job.setMapOutputKeyClass(keyToSend);
        // job.setMapOutputValueClass(valueToSend);
        // job.setOutputKeyClass(keyReduce);
        // job.setOutputValueClass(valueReduce);
        O.setOutputPath(job, new Path(jobOuputhPath));
        job.setNumReduceTasks(numReduce);
        job.waitForCompletion(true);
    }

    @SuppressWarnings({ "rawtypes" })
    protected <M extends Mapper, KM extends WritableComparable, VM extends Writable, R extends Reducer, KR extends WritableComparable, VR extends Writable, O extends FileOutputFormat<KR, VR>> void launchHadoopJob(
            String jobName, List<Class<M>> mapperClasses, String jobOuputhPath,
            Class<KM> keyToSend, Class<VM> valueToSend, Class<R> reducerClass,
            Class<KR> keyReduce, Class<VR> valueReduce, O outputFormat,
            Integer numReduce, Class mainClass, String... inputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(mainClass);
        job.setOutputFormatClass(outputFormat.getClass());
        job.setInputFormatClass(SequenceFileInputFormat.class);
        Path[] paths = new Path[inputPath.length];
        int i = 0;
        for (String s : inputPath) {
            paths[i] = new Path(s);
            MultipleInputs.addInputPath(job, new Path(s),
                    SequenceFileInputFormat.class, mapperClasses.get(i));
            i++;
        }
        job.setReducerClass(reducerClass);
        job.setMapOutputKeyClass(keyToSend);
        job.setMapOutputValueClass(valueToSend);
        job.setOutputKeyClass(keyReduce);
        job.setOutputValueClass(valueReduce);
        O.setOutputPath(job, new Path(jobOuputhPath));
        job.setNumReduceTasks(numReduce);
        job.waitForCompletion(true);
    }

    @SuppressWarnings("rawtypes")
    protected <K extends WritableComparable, V extends Writable> void launchHadoopCounterJob(
            Class<K> keyToSend, Class<V> valueToSend, String jobName,
            String jobOuputhPath, String inputPath, Class mainClass)
            throws IOException, ClassNotFoundException, InterruptedException {
        this.launchHadoopJob(jobName, CounterMap.class, jobOuputhPath,
                IntWritable.class, NullWritable.class, CounterReducer.class,
                IntWritable.class, NullWritable.class,
                new TextOutputFormat<IntWritable, NullWritable>(), 1,
                mainClass, inputPath);
    }

    protected static Float getMinutesFormMilis(long initTime, long endTime) {
        return (Float.valueOf(endTime) - Float.valueOf(initTime)) / 1000f / 60f;
    }

}
