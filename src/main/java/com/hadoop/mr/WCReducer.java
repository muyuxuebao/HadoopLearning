package com.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by muyux on 2015/11/30.
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //define a counter
        long counter = 0;
        //loop
        for (LongWritable value : values) {
            counter += value.get();
        }
        //write
        context.write(key, new LongWritable(counter));
    }
}
