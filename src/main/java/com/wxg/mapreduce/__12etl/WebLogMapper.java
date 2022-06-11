package com.wxg.mapreduce.__12etl;/*
    @author wxg
    @date 2021/5/28-21:20
    */


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取1行数据
        String line = value.toString();
        //2、解析日志
        boolean result = parseLog(line);
        //3、日志不合法退出
        if(!result) return;
        //4、日志合法就直接写出
        context.write(value, NullWritable.get());
    }

    public boolean parseLog(String line){
        //1 截取
        String[] fields = line.split(" ");
        //2 如果日志长度大于11， 就返回true
        return fields.length > 11;
    }
}
