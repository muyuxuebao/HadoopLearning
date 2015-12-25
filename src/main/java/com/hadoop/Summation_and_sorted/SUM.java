package com.hadoop.Summation_and_sorted;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by muyux on 2015/12/24.
 */
public class SUM {


    public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        private InfoBean v = new InfoBean();
        private Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String fields[] = line.split("\t");

            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double expenses = Double.parseDouble(fields[2]);

            k.set(account);

            v.set(account, income, expenses);
            context.write(k, v);

        }
    }


    public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {
        private InfoBean v = new InfoBean();

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            double income_sum = 0;
            double expenses_sum = 0;
            for (InfoBean value : values) {
                income_sum += value.getIncome();
                expenses_sum += value.getExpenses();
            }

            v.set("", income_sum, expenses_sum);
            context.write(key, v);


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SUM.class);

        job.setMapperClass(SumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));


        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
