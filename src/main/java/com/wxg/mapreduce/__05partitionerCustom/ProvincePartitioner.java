package com.wxg.mapreduce.__05partitionerCustom;/*
    @author wxg
    @date 2021/5/23-20:48
    */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    /**
     * getPartiton()方法在map()、context(k,v)之后调用
     * @param text 表示电话号码
     * @param flowBean 表示电话号码所代表的Bean对象
     * @param numPartitions 分区个数
     * @return 返回某个分区值
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
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
