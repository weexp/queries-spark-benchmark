package com.stratio.deep.benchmark.hdfs.hadoop.join.map;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.PageCountWritable;
import com.stratio.deep.benchmark.common.hadoop.model.PageWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;

public class PageCountJoinMapper extends
        Mapper<UUIDWritable, PageCountWritable, Text, RevisionPageCounter> {

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(UUIDWritable key, PageCountWritable value,
            Context context) throws IOException, InterruptedException {
        String title = value.getTitle();
        context.write(new Text(title), new RevisionPageCounter(title, new Date(
                value.getTs()), value.getPageCount(),
                new ContributorWritable(), BenchmarkConstans.BOOLEAN_NULL,
                new PageWritable(), BenchmarkConstans.STRING_NULL,
                BenchmarkConstans.STRING_NULL));
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
}
