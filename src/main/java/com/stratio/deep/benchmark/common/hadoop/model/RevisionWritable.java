package com.stratio.deep.benchmark.common.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.stratio.deep.benchmark.common.BenchmarkConstans;

public class RevisionWritable implements WritableComparable<RevisionWritable> {

    private UUIDWritable id;
    private ContributorWritable contributorWritable;
    private Boolean isMinor;
    private PageWritable pageWritable;
    private String text;
    private String redirection;
    private Date ts;

    public RevisionWritable() {
        this.contributorWritable = new ContributorWritable();
        this.isMinor = BenchmarkConstans.BOOLEAN_NULL;
        this.pageWritable = new PageWritable();
        this.text = BenchmarkConstans.STRING_NULL;
        this.redirection = BenchmarkConstans.STRING_NULL;
        this.ts = BenchmarkConstans.DATE_NULL;
        this.id = new UUIDWritable(BenchmarkConstans.UUID_BLANK);
    }

    public RevisionWritable(ContributorWritable contributorWritable,
            Boolean isMinor, PageWritable pageWritable, Text text,
            String redirection, String id, Long ts) {
        super();
        this.contributorWritable = contributorWritable;
        this.isMinor = isMinor;
        this.pageWritable = pageWritable;
        this.text = text.toString();
        this.redirection = redirection;
        this.id = new UUIDWritable(id);
        this.ts = new Date(ts);
    }

    public UUID getId() {
        return UUID.fromString(this.id.toString());
    }

    public void setId(UUIDWritable id) {
        this.id = id;
    }

    public Boolean getIsMinor() {
        return this.isMinor;
    }

    public void setIsMinor(Boolean isMinor) {
        this.isMinor = isMinor;
    }

    public Text getText() {
        return new Text(this.text);
    }

    public PageWritable getPageWritable() {
        return this.pageWritable;
    }

    public void setText(Text text) {
        this.text = text.toString();
    }

    public String getRedirection() {
        return this.redirection;
    }

    public void setRedirection(String redirection) {
        this.redirection = redirection;
    }

    public ContributorWritable getContributor() {
        return this.contributorWritable;
    }

    public void setContributorWritable(ContributorWritable contributorWritable) {
        this.contributorWritable = contributorWritable;
    }

    public void setPageWritable(PageWritable pageWritable) {
        this.pageWritable = pageWritable;
    }

    public Date getTs() {
        return this.ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.contributorWritable.write(out);
        out.writeBoolean(this.isMinor);
        this.pageWritable.write(out);
        Text.writeString(out, this.text);
        Text.writeString(out, this.redirection);
        out.writeLong(this.ts.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.contributorWritable.readFields(in);
        this.isMinor = in.readBoolean();
        this.pageWritable.readFields(in);
        try {
            this.text = Text.readString(in);
            this.ts = new Date(in.readLong());
        } catch (IndexOutOfBoundsException e) {
            this.text = " ";
            this.ts = new Date(0l);
        }
    }

    @Override
    public int compareTo(RevisionWritable other) {
        return this.id.toString().compareTo(other.getId().toString());
    }
}
