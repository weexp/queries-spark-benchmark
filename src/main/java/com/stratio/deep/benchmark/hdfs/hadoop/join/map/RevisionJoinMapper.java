package com.stratio.deep.benchmark.hdfs.hadoop.join.map;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.PageWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionWritable;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;

public class RevisionJoinMapper extends
        Mapper<UUIDWritable, RevisionWritable, Text, RevisionPageCounter> {

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(UUIDWritable key, RevisionWritable value, Context context)
            throws IOException, InterruptedException {
        if (null != value) {
            PageWritable pageWritable = value.getPageWritable();
            if (null != pageWritable) {
                context.write(
                        new Text(pageWritable.getTitle()),
                        new RevisionPageCounter(pageWritable.getTitle(), value
                                .getTs(), BenchmarkConstans.INT_NULL, value
                                .getContributor(), value.getIsMinor(),
                                pageWritable, value.getText().toString(), value
                                        .getRedirection()));
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
}