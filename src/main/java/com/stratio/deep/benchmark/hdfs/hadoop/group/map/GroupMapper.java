package com.stratio.deep.benchmark.hdfs.hadoop.group.map;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.stratio.deep.benchmark.common.hadoop.model.ContributorWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionWritable;
import com.stratio.deep.benchmark.common.hadoop.model.UUIDWritable;

public class GroupMapper extends
        Mapper<UUIDWritable, RevisionWritable, Text, NullWritable> {

    @Override
    protected void map(UUIDWritable key, RevisionWritable value, Context context)
            throws IOException, InterruptedException {
        ContributorWritable contributor = value.getContributor();
        if (null != contributor) {
            context.write(new Text(contributor.getUsername()),
                    NullWritable.get());
        }
    }

}
