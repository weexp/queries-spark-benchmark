package com.stratio.deep.benchmark.cassandra.hadoop.group.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.stratio.deep.benchmark.model.ContributorWritable;
import com.stratio.deep.benchmark.model.RevisionPageCounter;

public class HBaseGroup1Reduce extends
        Reducer<Text, RevisionPageCounter, ContributorWritable, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<RevisionPageCounter> values,
            Context context) throws IOException, InterruptedException {
        ContributorWritable keyToSend = null;
        int i = 0;
        for (RevisionPageCounter value : values) {
            if (null == keyToSend && null != value.getContributor()) {
                keyToSend = value.getContributor();
            }
            if (null != value.getPagecounts()) {
                i += value.getPagecounts();
            }
        }
        if (null != keyToSend) {
            context.write(keyToSend, new IntWritable(i));
        }
    }

}
