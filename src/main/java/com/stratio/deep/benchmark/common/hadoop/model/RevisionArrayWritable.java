package com.stratio.deep.benchmark.common.hadoop.model;

import org.apache.hadoop.io.ArrayWritable;

public class RevisionArrayWritable extends ArrayWritable {

    public RevisionArrayWritable() {
        super(RevisionWritable.class);
    }

}
