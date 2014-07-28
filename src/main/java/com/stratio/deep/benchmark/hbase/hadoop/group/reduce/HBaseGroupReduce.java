package com.stratio.deep.benchmark.hbase.hadoop.group.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionArrayWritable;
import com.stratio.deep.benchmark.common.hadoop.model.RevisionWritable;

public class HBaseGroupReduce extends
        Reducer<Text, RevisionWritable, Text, RevisionArrayWritable> {

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<RevisionWritable> values,
            Context context) throws IOException, InterruptedException {
        RevisionArrayWritable revisionArray = new RevisionArrayWritable();
        revisionArray.set(Iterables.toArray(values,
                RevisionWritable.class));
        context.write(key, revisionArray);
    }

}
