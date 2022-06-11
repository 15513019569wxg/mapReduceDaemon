package com.wxg.mapreduce.__07区内排序;/*
    @author wxg
    @date 2021/5/23-20:48
    */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition( FlowBean flowBean, Text text, int numPartitions) {
        //text是手机号
        String phone = text.toString();

        //取出手机号的前三位
        String prePhone = phone.substring(0, 3);

        //划定分区文件
        int partition;
        switch (prePhone) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
                break;
        }
        return partition;
    }
}
