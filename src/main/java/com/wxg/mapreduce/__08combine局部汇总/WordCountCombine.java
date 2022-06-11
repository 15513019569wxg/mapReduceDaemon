package com.wxg.mapreduce.__08combine局部汇总;/*
    @author wxg
    @date 2021/5/24-0:52
    */


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordCountCombine extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) sum += value.get();
        outV.set(sum);
        //写出
        context.write(key, outV);
    }
}
