package com.wxg.mapreduce.__10reducejoin;/*
    @author wxg
    @date 2021/5/26-0:28
    */


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String fileName;
    private final Text outK = new Text();
    private final TableBean outV = new TableBean();

    @Override
    protected void setup(Context context) {
        //获取不同的文件名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
        System.out.println(fileName);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //判断是哪个文件，然后针对不同的文件进行操作
        String[] split = line.split("\t");
        if(fileName.contains("order")){
            //封装outK
            outK.set(split[1]);
            //封装outV
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else{
            //封装outK
            outK.set(split[0]);
            //封装outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        //写出KV
        context.write(outK, outV);
    }
}
