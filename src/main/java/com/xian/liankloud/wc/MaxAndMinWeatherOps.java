package com.xian.liankloud.wc;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 求一年中的最高温度和最低温度
 一天中的最高温度在一行的位置(103, 108)
 一天中的最低温度在一行的位置(111, 116)
 */
public class MaxAndMinWeatherOps {
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String jobName = MaxAndMinWeatherOps.class.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        //java程序运行是以jar来运行的，
        job.setJarByClass(MaxAndMinWeatherOps.class);

        //设置输入
        //设置了对输入文件进行格式化操作的类
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, "input/hadoop/mr/010010-99999-2015.op");//设置mr程序的输入源
        //设置map
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        //设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/hadoop/mr"));
        //设置reduce
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //设置reducer task num的个数
        job.setNumReduceTasks(1);

        //提交job到集群中运行
        job.waitForCompletion(true);

    }

    static class WeatherMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String maxStr = line.substring(103, 108).replaceAll("\\*", "").trim();
            String minStr = line.substring(111, 116).replaceAll("\\*", "").trim();
            try {
                double max = Double.valueOf(maxStr);
                double min = Double.valueOf(minStr);

                //写出去
                context.write(new Text("MAX"), new DoubleWritable(max));
                context.write(new Text("MIN"), new DoubleWritable(min));
            } catch (NumberFormatException e) {
                return;
            }
        }
    }

    static class WeatherReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double MAX = Double.MIN_VALUE;
            double MIN = Double.MAX_VALUE;

            if(key.toString().equalsIgnoreCase("MAX")) { //求最大值
                for(DoubleWritable dw : values) {
                    if(dw.get() > MAX) {
                        MAX = dw.get();
                    }
                }
                context.write(key, new DoubleWritable(MAX));
            } else { //求最小值
                for(DoubleWritable dw : values) {
                    if(dw.get() < MIN) {
                        MIN = dw.get();
                    }
                }
                context.write(key, new DoubleWritable(MIN));
            }
        }
    }



}
