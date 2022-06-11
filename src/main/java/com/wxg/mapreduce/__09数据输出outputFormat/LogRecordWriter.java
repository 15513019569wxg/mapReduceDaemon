package com.wxg.mapreduce.__09数据输出outputFormat;/*
    @author wxg
    @date 2021/5/24-23:56
    */


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;


public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public LogRecordWriter(TaskAttemptContext job) {
        // 创建两条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            atguigu = fs.create(new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\atguigu.log"));
            other = fs.create(new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if(log.contains("atguigu")) atguigu.writeBytes(log + "\n");
        else other.writeBytes(log + "\n");
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
