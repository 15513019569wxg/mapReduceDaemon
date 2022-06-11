package com.wxg.mapreduce.__11mapjoin;/*
    @author wxg
    @date 2021/5/28-20:10
    */


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private final Map<String, String> pdMap = new HashMap<>();
    private final Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        //通过缓存文件得到小表数据pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统对象，并打开流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fileSystem.open(path);

        //通过包装流转换为reader, 方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            // 切割一行（01  小米）
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }

        System.out.println(pdMap.toString());

        //关闭流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取大表数据（1001   01  1）
        String[] fields = value.toString().split("\t");

        //通过大表每行数据的pid取获取pdMap里面的pname
        String pname = pdMap.get(fields[1]);

        //将大表每行数据的pid替换为pname
        text.set(fields[0] + "\t" + pname + "\t" + fields[2]);

        //写出
        context.write(text, NullWritable.get());
    }
}
