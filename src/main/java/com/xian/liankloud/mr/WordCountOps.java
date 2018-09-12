package com.xian.liankloud.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 需求：
 *  统计在hdfs上面/input/hadoop/mr/data.log中每一个单词出现的次数，
 * 将统计结果存放到/output/hadoop/mr/wc
 *
 * 编程步骤：
 *  在main函数中执行我们编写的mr(map reduce)程序
 *
 *  map
 *      1、创建一个类，去继承org.apache.hadoop.mapreduce.Mapper类，复写其中的map()函数，完成map阶段的任务
 *      2、确定4个类型参数
 *      3、复写其中的map方法
 *  reduce
 *      1、创建一个类，去继承org.apache.hadoop.mapreduce.Reducer类，复写其中的reduce()函数，完成reduce阶段的汇总任务
 *      2、确定4个类型参数
 *      3、复写其中的reduce方法
 *  对map和reduce进行组织
 *
 *
 *  作业的概念
 *  我们编写的MR程序，是一个java进程，在hadoop集群中运行的时候，是以一个个的Job或者Application的这形态存在的
 */
public class WordCountOps {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String jobName = WordCountOps.class.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        //java程序运行是以jar来运行的，
        job.setJarByClass(WordCountOps.class);

        //设置输入
        //设置了对输入文件进行格式化操作的类
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, "/input/hadoop/mr/LICENSE.txt");//设置mr程序的输入源
        //设置map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/hadoop/mr/wc"));
        //设置reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置reducer task num的个数
        job.setNumReduceTasks(1);

        //提交job到集群中运行
        job.waitForCompletion(true);
    }

    /**
     * 自定义map
     * 四个类型参数
     * ---------输入参数
     * k1  ---->Long        ------>LongWritable
     * v1  ---->String      ------>Text
     * ---------输出参数
     * k2  ---->String      ------>Text
     * v2  ---->Integer     ------>IntWritable
     *
     * 在hadoop中重新对Integer/Long/Double/String等等，这些序列化的类重新进行的封装，为的是适应大数据环境下的数据的传输
     */
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
        /**
         * 每一行的内容调用一次该map函数
         * @param key       就是k1，行偏移量
         * @param value     就是v1，行内容
         * @param context   map阶段操作输入输出，以及获取程序配置信息的上下文对象
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String v1 = value.toString();//hei hei hei
            String msg1 = "map输入\t"+key.get()+","+v1;
            System.out.println(msg1);
            logger.debug("-------" + msg1);
            String[] words = v1.split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
                String msg2 = "map输出\t"+word.toString()+","+ 1;
                System.out.println(msg2);
                logger.debug("-------" + msg2);
                logger.debug(msg2);
            }
        }
    }

    /**
     * 自定义reduce
     * 四个类型参数
     * ---------输入参数
     * k2  ----->String      ------>Text
     * v2  ----->Integer     ------>IntWritable
     *  ---------输出参数
     * k3  ----->String      ------>Text
     * v3  ----->Integer     ------>IntWritable
     */
    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        Logger logger = LoggerFactory.getLogger(WordCountReducer.class);

        /**
         *
         * @param k2       就是map输出的k2
         * @param v2s      就是经过shuffle之后的v2的集合列表
         * @param context  reduce阶段操作输入输出，以及获取程序配置信息的上下文对象
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context)
                throws IOException, InterruptedException {
            System.out.println("reduce输入分组k2\t"+k2.toString());
            int sum = 0;
            for(IntWritable iw : v2s) {
                System.out.println("reduce输入分组k2对应的v2\t" + iw.get());
                sum += iw.get();
            }
            /**
             * 将k3和v3写出去
             * 其中k3和k2是同一个值
             */
            context.write(k2, new IntWritable(sum));
            System.out.println("reduce输出\t" + k2.toString() + "," + new IntWritable(sum).get());

        }
    }

}
