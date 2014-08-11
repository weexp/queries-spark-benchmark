package com.stratio.deep.benchmark.hdfs.hadoop.tex.pagecounts;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.BenchmarkConstans;
import com.stratio.deep.benchmark.common.hadoop.model.DataType;
import com.stratio.deep.benchmark.common.hadoop.model.PageCountWritable;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;

public class TextToEntityPageCountMapper extends
        Mapper<IntWritable, Text, UUIDWritable, PageCountWritable> {

    @Override
    protected void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        PageCountWritable pg = new PageCountWritable(new Long(this.nvl(
                split[3], DataType.Long)), split[2], new Integer(this.nvl(
                split[1], DataType.Integer)));
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
