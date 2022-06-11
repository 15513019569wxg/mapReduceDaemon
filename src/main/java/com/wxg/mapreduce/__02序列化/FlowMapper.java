package com.wxg.mapreduce.__02序列化;/*
    @author wxg
    @date 2021/5/20-17:51
    */


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    // 创建键值对
    private final Text outK = new Text();
    private final FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行信息
        String line = value.toString();
        //切割
        String[] split = line.split("\t");
        // 抓取目标数据（手机号、上行流量、下行流量）
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        //封装
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        //写出
        context.write(outK, outV);
    }
}
