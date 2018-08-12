package com.yinliang.hadoop.mr.inverted_index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by muyux on 2015/12/25.
 */
public class Inverted_index {

    private static class Inverted_index_mapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String ss[] = line.split("( )+");

            FileSplit filesplit = (FileSplit) context.getInputSplit();
            String path = filesplit.getPath().toString();

            for (String s : ss) {
                k.set(s + "->" + path);
                v.set("1");
                context.write(k, v);
            }
        }
    }

    private static class Inverted_index_combiner extends Reducer<Text, Text, Text, Text> {
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }

            v.set(String.valueOf(count));
            context.write(key, v);
        }
    }

    private static class Inverted_index_reducer extends Reducer<Text, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int times = Integer.parseInt(values.iterator().next().toString());
            String its[] = key.toString().split("->");

            k.set(its[0]);
            v.set(its[1] + ", " + String.valueOf(times));

            context.write(k, v);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        //设置jar
        job.setJarByClass(Inverted_index.class);

        //设置Mapper相关的属性
        job.setMapperClass(Inverted_index_mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));//words.txt

        //设置Reducer相关属性
        job.setReducerClass(Inverted_index_reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setCombinerClass(Inverted_index_combiner.class);

        //提交任务
        job.waitForCompletion(true);
    }
}
