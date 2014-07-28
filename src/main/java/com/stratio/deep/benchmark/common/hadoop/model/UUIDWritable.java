package com.stratio.deep.benchmark.common.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.WritableComparable;

public class UUIDWritable implements WritableComparable<UUIDWritable> {

    private UUID value;

    public UUIDWritable(long mostSignificantBits, long leastSignificantBits) {
        this.value = new UUID(mostSignificantBits, leastSignificantBits);
    }

    public UUIDWritable(String stringRep) {
        this.value = UUID.fromString(stringRep);
    }

    public UUIDWritable() {
        this.value = UUID.randomUUID();
    }

    public UUIDWritable(UUID uuid) {
        this.value = uuid;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object obj) {
        UUIDWritable other = (UUIDWritable) obj;
        return this.value.getMostSignificantBits() == other.value
                .getMostSignificantBits()
                && this.value.getLeastSignificantBits() == other.value
                        .getLeastSignificantBits();
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.value.getMostSignificantBits());
        out.writeLong(this.value.getLeastSignificantBits());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        long mostSignificantBits = in.readLong();
        long leastSignificantBits = in.readLong();
        this.value = new UUID(mostSignificantBits, leastSignificantBits);
    }

    @Override
    public int compareTo(UUIDWritable o) {
        return this.value.compareTo(o.value);
    }

}
