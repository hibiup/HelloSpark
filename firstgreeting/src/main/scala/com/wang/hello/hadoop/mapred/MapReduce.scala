package com.wang.hello.hadoop.mapred

/**
  * Created by wangji on 2016/3/24.
  */

import org.apache.log4j.Logger;

import java.util._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
  * Mapper 泛型参数：[输入键,输入值,输出键,输出值]
  *
  * LongWritable 相当于 Long，是行数偏移量. Text 相当于String，是每一行的内容，IntWritable相当于 Int，输出统计结果
  */
class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    val logger = Logger.getLogger(this.getClass)

    private val one = new IntWritable(1);
    private val word = new Text();

    def map(key: LongWritable, value: Text, context: Context) {
        val line = value.toString
        val tokenizer = new StringTokenizer(line)
        while (tokenizer.hasMoreTokens) {
            word.set(tokenizer.nextToken)
            context.write(word, one)
        }
    }
}

class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] {
    val logger = Logger.getLogger(this.getClass)

    def reduce(key: Text, values: Iterator[IntWritable], context: Context) {
        context.write(key, new IntWritable(count(0, values)))

        def count(sum: Int, vs: Iterator[IntWritable]): Int =
            if (vs.hasNext)
                count(sum + vs.next.get, vs)
            else
                sum
    }
}

class WordCount(username: String, groupname: String) {
    val logger = Logger.getLogger(this.getClass)

    // HDFS configuration
    val conf = new Configuration()
    conf.addResource("core-site.xml")
    conf.addResource("hdfs-site.xml")
    System.setProperty("HADOOP_USER_NAME", username)
    System.setProperty("HADOOP_GROUP_NAME", groupname)

    def count(args: Array[String]) = {
        // Job configuration
        val job = Job.getInstance(conf, "WordCount")
        job.setJarByClass(this.getClass)
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])

        // Setup map and reduce
        job.setMapperClass(classOf[Map])
        job.setCombinerClass(classOf[Reduce])
        job.setReducerClass(classOf[Reduce])

        // Setup input and output
        FileInputFormat.setInputPaths(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))

        /**
          * Job类中提供了两种启动Job的方式：
          *
          * 1. submit()
          *
          * submit函数会把Job提交给对应的Cluster，然后不等待Job执行结束就立刻返回。同时会把Job实例的状态设置为JobState.RUNNING，从而来表示
          * Job正在进行中。然后在Job运行过程中，可以调用getJobState()来获取Job的运行状态。
          *
          * 2. waitForCompletion(boolean)
          *
          * waitForCompletion函数会提交Job到对应的Cluster，并等待Job执行结束。函数的boolean参数表示是否打印Job执行的相关信息。返回的结果是
          * 一个boolean变量，用来标识Job的执行结果。
          */
        job.waitForCompletion(true) match {case true =>0; case _ => 1}
    }
}

object WordCount {
    def main(args: Array[String]) {
        val wordCount = new WordCount("root", "supergroup")
        wordCount.count(args)
    }
}