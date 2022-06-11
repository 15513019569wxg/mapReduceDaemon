package com.wxg.mapreduce.__10reducejoin;/*
    @author wxg
    @date 2021/5/26-1:07
    */


import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 01   1001  1         order
        // 01   1004 4         order
        // 01            小米    pd
        //创建一个集合和一个TableBean对象，分别用于接收order文件的多个TableBean对象和pd文件的一个TableBean对象
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {//订单表
                //创建一个临时TableBean对象接收value
                TableBean tmpOrderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpOrderBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
                //将临时TableBean对象添加到集合orderBeans
                orderBeans.add(tmpOrderBean);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //遍历集合orderBeans, 替换掉每个orderBean的pid为pname，然后写出
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());
            //写出修改后的orderBean对象
            context.write(orderBean, NullWritable.get());
        }
    }
}
