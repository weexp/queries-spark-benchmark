package com.stratio.deep.benchmark.common.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.stratio.deep.benchmark.common.BenchmarkConstans;

public class PageWritable implements Writable {

    private String namespace;
    private String title;
    private String fullTitle;
    private Integer id;
    private Boolean isRedirect;
    private Text restrictions;

    public PageWritable() {
        super();
        this.namespace = BenchmarkConstans.STRING_NULL;
        this.title = BenchmarkConstans.STRING_NULL;
        this.fullTitle = BenchmarkConstans.STRING_NULL;
        this.id = BenchmarkConstans.INT_NULL;
        this.isRedirect = BenchmarkConstans.BOOLEAN_NULL;
        this.restrictions = BenchmarkConstans.TEXT_NULL;
    }

    public PageWritable(String namespace, String title, String fullTitle,
            Integer id, Boolean isRedirect, Text restrictions) {
        super();
        this.namespace = namespace;
        this.title = title;
        this.fullTitle = fullTitle;
        this.id = id;
        this.isRedirect = isRedirect;
        this.restrictions = restrictions;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getFullTitle() {
        return this.fullTitle;
    }

    public void setFullTitle(String fullTitle) {
        this.fullTitle = fullTitle;
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Boolean getIsRedirect() {
        return this.isRedirect;
    }

    public void setIsRedirect(Boolean isRedirect) {
        this.isRedirect = isRedirect;
    }

    public Text getRestrictions() {
        return this.restrictions;
    }

    public void setRestrictions(Text restrictions) {
        this.restrictions = restrictions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.namespace);
        Text.writeString(out, this.title);
        Text.writeString(out, this.fullTitle);
        out.write(this.id);
        out.writeBoolean(this.isRedirect);
        this.restrictions.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.namespace = Text.readString(in);
        this.title = Text.readString(in);
        this.fullTitle = Text.readString(in);
        this.id = in.readInt();
        this.isRedirect = in.readBoolean();
        try {
            this.restrictions.readFields(in);
        } catch (IndexOutOfBoundsException e) {
            this.restrictions = new Text("");
        }
    }

}
