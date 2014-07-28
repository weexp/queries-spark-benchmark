package com.stratio.deep.benchmark.common.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.stratio.deep.benchmark.common.BenchmarkConstans;

public class PageCountWritable implements WritableComparable<PageCountWritable> {

    private Long ts;
    private String title;
    private Integer pageCount;

    public PageCountWritable(Long ts, String title, Integer pageCount) {
        this.ts = ts;
        this.title = title;
        this.pageCount = pageCount;
    }

    public PageCountWritable() {
        this.ts = BenchmarkConstans.LONG_NULL;
        this.title = BenchmarkConstans.STRING_NULL;
        this.pageCount = BenchmarkConstans.INT_NULL;
    }

    public Long getTs() {
        return this.ts;
    }

    public String getTitle() {
        return this.title;
    }

    public Integer getPageCount() {
        return this.pageCount;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setPageCount(Integer pageCount) {
        this.pageCount = pageCount;
    }

    public static PageCountWritable build(Long ts, String title,
            Integer pageCount) {
        PageCountWritable writable = new PageCountWritable();
        if (null != ts) {
            writable.setTs(ts);
        }
        if (null != title) {
            writable.title = title;
        }
        if (null != pageCount) {
            writable.setPageCount(pageCount);
        }
        return writable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.ts);
        Text.writeString(out, this.title);
        out.writeInt(this.pageCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.ts = in.readLong();
        this.title = Text.readString(in);
        this.pageCount = in.readInt();
    }

    @Override
    public int compareTo(PageCountWritable other) {
        return this.title.compareTo(other.getTitle());
    }
}
