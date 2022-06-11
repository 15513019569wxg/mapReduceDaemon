package com.wxg.mapreduce.__07区内排序;/*
    @author wxg
    @date 2021/5/20-17:33
    */

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1、定义类实现writable接口
 * 2、重写序列化和反序列化方法
 * 3、重写空参构造
 * 4、toString方法
 */

public class FlowBean implements WritableComparable<FlowBean> {

    //定义需要计算的字段
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow(){
        sumFlow = this.upFlow + this.downFlow;
    }


    //空参构造
    public FlowBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return  upFlow + "\t" + downFlow + "\t" + sumFlow + "\t";
    }

    @Override
    public int compareTo(FlowBean o) {
        if (this.sumFlow > o.sumFlow) return -1;
        else if(this.sumFlow < o.sumFlow) return 1;
        else return Long.compare(this.upFlow, o.upFlow);
    }
}
