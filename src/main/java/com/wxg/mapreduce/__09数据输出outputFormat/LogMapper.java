package com.wxg.mapreduce.__09数据输出outputFormat;/*
    @author wxg
    @date 2021/5/24-23:38
    */


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //在这里不做任何处理，直接将网址输出，（http://www.baidu.com, NUllWritable）
        context.write(value, NullWritable.get());
    }
}
