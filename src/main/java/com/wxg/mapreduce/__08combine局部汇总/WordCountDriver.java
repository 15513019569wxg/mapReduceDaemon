package com.wxg.mapreduce.__08combine局部汇总;/*
    @author wxg
    @date 2021/5/19-1:49
    */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        
        //2 关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);
        
        //3 关联Mapper和Reduce的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        
        //4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //5 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //将combinereduce类与驱动关联起来
//        job.setCombinerClass(WordCountCombine.class);
        //可以直接使用Reducer，不用手写combine, 因为这两个类的方法和属性完全一样
        job.setCombinerClass(WordCountReducer.class);
//        job.setNumReduceTasks(0);

        //6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\hello.txt"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\homework\\withoutcombine"));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\homework\\combine"));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\homework\\combineReduceZero"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\combineSameASReducer"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);
    }
}
