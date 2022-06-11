package com.wxg.mapreduce.__09数据输出outputFormat;/*
    @author wxg
    @date 2021/5/24-23:45
    */


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //如果有相同的数据,防止丢失数据，这里要使用循环
        // http://www.baidu.com
        // http://www.baidu.com
        for (NullWritable value : values) context.write(key, NullWritable.get());
    }
}
