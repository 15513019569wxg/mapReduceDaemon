package com.wxg.mapreduce.__12etl;/*
    @author wxg
    @date 2021/5/28-21:30
    */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WebLogDriver {
    private static String[] args;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        WebLogDriver.args = args;
        //输入输出路径需要根据自己电脑上实际的输入输出路径进行设置
       args = new String[]{"D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\资料\\11_input\\inputlog", "D:\\大数据项目资料\\尚硅谷大数据技术之hadoop\\3.x\\homework\\etlLog"};

        // 1、获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2、加载jar包路径
        job.setJarByClass(WebLogDriver.class);

        //3、关联map
        job.setMapperClass(WebLogMapper.class);

        //4、设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置reducetask个数为0
        job.setNumReduceTasks(0);

        //5、设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6、提交
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0: 1);
    }

    public static String[] getArgs() {
        return args;
    }

    public static void setArgs(String[] args) {
        WebLogDriver.args = args;
    }
}
