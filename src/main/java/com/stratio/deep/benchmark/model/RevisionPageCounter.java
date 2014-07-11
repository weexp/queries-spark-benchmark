package com.stratio.deep.benchmark.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.stratio.deep.benchmark.BenckmarkConstans;

public class RevisionPageCounter implements
        WritableComparable<RevisionPageCounter> {

    private String title;
    private Date ts;
    private Integer pagecounts;
    private ContributorWritable contributorWritable;
    private Boolean isMinor;
    private PageWritable pageWritable;
    private String text;
    private String redirection;

    public RevisionPageCounter() {
        this.title = BenckmarkConstans.STRING_NULL;
        this.ts = BenckmarkConstans.DATE_NULL;
        this.pagecounts = BenckmarkConstans.INT_NULL;
        this.contributorWritable = new ContributorWritable();
        this.isMinor = BenckmarkConstans.BOOLEAN_NULL;
        this.pageWritable = new PageWritable();
        this.text = BenckmarkConstans.STRING_NULL;
        this.redirection = BenckmarkConstans.STRING_NULL;
    }

    public RevisionPageCounter(String title, Date ts, Integer pagecounts,
            ContributorWritable contributorWritable, Boolean isMinor,
            PageWritable pageWritable, String text, String redirection) {
        this.title = title;
        this.ts = ts;
        this.pagecounts = pagecounts;
        this.contributorWritable = contributorWritable;
        this.isMinor = isMinor;
        this.pageWritable = pageWritable;
        this.text = text;
        this.redirection = redirection;
    }

    public ContributorWritable getContributorWritable() {
        return this.contributorWritable;
    }

    public void setContributorWritable(ContributorWritable contributorWritable) {
        this.contributorWritable = contributorWritable;
    }

    public PageWritable getPageWritable() {
        return this.pageWritable;
    }

    public void setPageWritable(PageWritable pageWritable) {
        this.pageWritable = pageWritable;
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Date getTs() {
        return this.ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    public Integer getPagecounts() {
        return this.pagecounts;
    }

    public void setPagecounts(Integer pagecounts) {
        this.pagecounts = pagecounts;
    }

    public Boolean getIsMinor() {
        return this.isMinor;
    }

    public void setIsMinor(Boolean isMinor) {
        this.isMinor = isMinor;
    }

    public String getText() {
        return this.text;
    }

    public void setText(String text) {
        this.text = text;
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

    public PageWritable getPage() {
        return this.pageWritable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.title);
        out.writeLong(this.ts.getTime());
        out.writeInt(this.pagecounts);
        this.contributorWritable.write(out);
        out.writeBoolean(this.isMinor);
        this.pageWritable.write(out);
        Text.writeString(out, this.text);
        Text.writeString(out, this.redirection);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.title = Text.readString(in);
        this.ts = new Date(in.readLong());
        this.pagecounts = in.readInt();
        this.contributorWritable.readFields(in);
        this.isMinor = in.readBoolean();
        this.pageWritable.readFields(in);
        this.text = Text.readString(in);
        this.redirection = Text.readString(in);
    }

    @Override
    public int compareTo(RevisionPageCounter other) {
        return this.title.compareTo(other.getTitle());
    }
}
