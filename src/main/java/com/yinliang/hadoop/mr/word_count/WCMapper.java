package com.yinliang.hadoop.mr.word_count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by muyux on 2015/11/30.
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //accept
        String line = value.toString();
        //split
        String words[] = line.split(" ");
        //loop
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
