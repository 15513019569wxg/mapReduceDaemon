package com.wxg.mapreduce.__11mapjoin;/*
    @author wxg
    @date 2021/5/28-19:57
    */



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        // 1、获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置加载jar包路径
        job.setJarByClass(MapJoinDriver.class);

        //3、关联Mapper
        job.setMapperClass(MapJoinMapper.class);

        //4、设置Mapper输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5、设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //加载缓存数据
        job.addCacheFile(new URI("file:///D:/大数据项目资料/尚硅谷大数据技术之hadoop/3.x/资料/11_input/tablecache/pd.txt"));
        //Map端Join的逻辑不需要使用Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        //6、设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\资料\\11_input\\inputtable2"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\mapJoin1"));

        //7、提交
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0: 1);
    }
}
