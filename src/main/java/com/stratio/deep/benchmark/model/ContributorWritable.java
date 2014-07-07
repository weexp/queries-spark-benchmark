package com.stratio.deep.benchmark.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.stratio.deep.benchmark.BenckmarkConstans;

public class ContributorWritable implements
        WritableComparable<ContributorWritable> {

    private Integer id;
    private String username;
    private Boolean isAnonymous;

    public ContributorWritable(Integer id, String username, Boolean isAnonymous) {
        super();
        this.id = id;
        this.username = username;
        this.isAnonymous = isAnonymous;
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Boolean getIsAnonymous() {
        return this.isAnonymous;
    }

    public void setIsAnonymous(Boolean isAnonymous) {
        this.isAnonymous = isAnonymous;
    }

    public ContributorWritable() {
        super();
        this.id = BenckmarkConstans.INT_NULL;
        this.username = BenckmarkConstans.STRING_NULL;
        this.isAnonymous = BenckmarkConstans.BOOLEAN_NULL;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        Text.writeString(out, this.username);
        out.writeBoolean(this.isAnonymous);
    }

    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.username = Text.readString(in);
        this.isAnonymous = in.readBoolean();
    }

    public int compareTo(ContributorWritable other) {
        return this.id.compareTo(other.getId());
    }

}
