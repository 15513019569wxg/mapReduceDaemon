package com.wxg.mapreduce.__10reducejoin;/*
    @author wxg
    @date 2021/5/26-1:27
    */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 关联本Driver程序的jar
        job.setJarByClass(TableDriver.class);

        //3 关联Mapper和Reduce的jar
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        //4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //5 设置最终输出的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\资料\\11_input\\inputtable"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\reducerjoin"));

        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);
    }
}
