package com.wxg.mapreduce.__13compress;/*
    @author wxg
    @date 2021/5/19-1:49
    */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


@SuppressWarnings("AlibabaRemoveCommentedCode")
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();

        //开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        //设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

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
        
        //6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\hello.txt"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\homework\\outputAfterReduceMapCompressBZip"));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\homework\\outputAfterReduceMapCompressGzip"));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\homework\\outputAfterReduceMapCompressDezip"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\outputAfterReduceMapCompressSnazip"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        //设置reduce端压缩的方式
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        //会报异常，需要Linux版本和集群环境支持
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);
    }


}
