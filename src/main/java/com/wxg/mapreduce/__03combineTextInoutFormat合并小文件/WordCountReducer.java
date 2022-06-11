package com.wxg.mapreduce.__03combineTextInoutFormat合并小文件;/*
    @author wxg
    @date 2021/5/19-1:52
    */


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values)  {
            // 累加求和
            sum += value.get();  //get()将其转化为int类型
        }
        //将int转换为IntWritable类型
        outV.set(sum);
        //写出结果
        context.write(key, outV);

    }
}
