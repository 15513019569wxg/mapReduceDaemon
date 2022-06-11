package com.wxg.mapreduce.__02序列化;/*
    @author wxg
    @date 2021/5/20-19:08
    */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置jar包
        job.setJarByClass(FlowDriver.class);

        //关联mapper和Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //关联mapper 输出key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //设置最终数据输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\资料\\11_input\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\hadoop大数据3.x\\homework\\writableOutput"));

        //提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
