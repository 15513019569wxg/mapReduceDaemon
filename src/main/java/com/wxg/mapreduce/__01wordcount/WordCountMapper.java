package com.wxg.mapreduce.__01wordcount;/*
    @author wxg
    @date 2021/5/19-1:51
    */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

//public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    //减少性能开销
    private final Text outK = new Text();
    //每次都将值的出现次数计为1，最后再进行聚合
    private final IntWritable outV = new IntWritable(1);

    /**
     *   Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     * @param key  文本
     * @param value 出现次数
     * @param context 上下文环境
     * @throws IOException  IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取每行数据
        String line = value.toString();
        //将每行数据进行切割，得到每个单词
        String[] words = line.split(" ");
        for (String word : words) {
            //将String类型修改为Text类型
            outK.set(word);
            context.write(outK, outV);
        }
    }
}
