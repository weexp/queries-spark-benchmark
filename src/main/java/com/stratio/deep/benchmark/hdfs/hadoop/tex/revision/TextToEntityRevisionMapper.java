package com.stratio.deep.benchmark.hdfs.hadoop.tex.revision;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.DataType;
import com.stratio.deep.benchmark.common.hadoop.model.PageWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionWritable;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;

public class TextToEntityRevisionMapper extends
        Mapper<IntWritable, Text, UUIDWritable, RevisionWritable> {

    @Override
    protected void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        ContributorWritable contributorWritable = new ContributorWritable(
                new Integer(split[7]), split[8], new Boolean(split[9]));
        PageWritable pageWritable = new PageWritable(split[2], split[4],
                split[3], new Integer(this.nvl(split[1], DataType.Integer)),
                new Boolean(this.nvl(split[6], DataType.Boolean)), new Text(
                        split[5]));
        RevisionWritable pg = new RevisionWritable(contributorWritable,
                new Boolean(this.nvl(split[10], DataType.Boolean)),
                pageWritable, new Text(split[13]), split[11], split[13],
                new Long(this.nvl(split[12], DataType.Long)));
        context.write(new UUIDWritable(split[0]), pg);
    }

    public String nvl(String text, DataType type) {
        if (null != text && text.equals("")) {
            switch (type) {
            case Boolean:
                return BenchmarkConstans.BOOLEAN_NULL.toString();
            case Integer:
                return BenchmarkConstans.INT_NULL.toString();
            case Long:
                return BenchmarkConstans.LONG_NULL.toString();
            default:
                return null;
            }
        }
        return null;
    }
}
