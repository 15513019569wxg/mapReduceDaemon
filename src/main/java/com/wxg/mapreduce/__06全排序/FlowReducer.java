package com.wxg.mapreduce.__06全排序;/*
    @author wxg
    @date 2021/5/20-18:03
    */


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer< FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) context.write(value, key);
    }
}
