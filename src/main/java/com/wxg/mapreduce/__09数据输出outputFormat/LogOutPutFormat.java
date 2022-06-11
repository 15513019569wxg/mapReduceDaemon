package com.wxg.mapreduce.__09数据输出outputFormat;/*
    @author wxg
    @date 2021/5/24-23:53
    */


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogOutPutFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) {
        final LogRecordWriter logRecordWriter;
        //这里会调用自定义的类
        logRecordWriter = new LogRecordWriter(job);
        return logRecordWriter;
    }
}
