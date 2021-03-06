package com.stratio.deep.benchmark.cassandra.hadoop.join.reduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.stratio.deep.benchmark.common.hadoop.model.RevisionPageCounter;

public class JoinReducer extends
        Reducer<Text, RevisionPageCounter, RevisionPageCounter, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<RevisionPageCounter> values,
            Context context) throws IOException, InterruptedException {
        RevisionPageCounter revisionPageCounter = new RevisionPageCounter();
        revisionPageCounter.setTitle(key.toString());

        for (RevisionPageCounter value : values) {
            if (null != value.getPagecounts()) {
                revisionPageCounter.setPagecounts(value.getPagecounts());
            }

            if (null != value.getContributor()) {
                revisionPageCounter.setContributorWritable(value
                        .getContributor());
            }
            if (null != value.getIsMinor()) {
                revisionPageCounter.setIsMinor(value.getIsMinor());

            }
            if (null != value.getPageWritable()) {
                revisionPageCounter.setPageWritable(value.getPageWritable());
            }
            if (null != value.getRedirection()) {
                revisionPageCounter.setRedirection(value.getRedirection());
            }
            if (null != value.getText()) {
                revisionPageCounter.setText(value.getText());
            }
            if (null != value.getTitle()) {
                revisionPageCounter.setTitle(value.getTitle());
            }
            if (null != value.getTs()) {
                revisionPageCounter.setTs(value.getTs());
            }
        }
        context.write(revisionPageCounter, NullWritable.get());
    }
}
