package com.wxg.mapreduce.__01wordcount;/*
    @author wxg
    @date 2021/5/19-1:52
    */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable outV = new IntWritable();

    /**
     * This method is called once for each key. Most applications will define their reduce class by overriding this method.
     * The default implementation is an identity function.
     * @param key  某个key值, 比如atguigu
     * @param values  这是一个迭代器，里面装的是[1, 1, 1.......]
     * @param context  上下文环境
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values)  {
            // 累加求和
            sum += value.get();  //get()将其转化为int类型
        }
        //将int转换为IntWritable类型
        outV.set(sum);
        //写出结果
        context.write(key, outV);
    }
}
