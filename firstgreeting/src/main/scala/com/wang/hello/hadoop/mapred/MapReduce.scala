package com.wang.hello.hadoop.mapred

/**
  * Created by wangji on 2016/3/24.
  */

import org.apache.log4j.Logger;

import java.io.IOException
import java.util._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

/**
  * Mapper 泛型参数：[输入键,输入值,输出键,输出值]
  *
  * LongWritable 相当于 Long，是行数偏移量. Text 相当于String，是每一行的内容，IntWritable相当于 Int，输出统计结果
  */
class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
    val logger = Logger.getLogger(this.getClass)

    private val one = new IntWritable(1);
    private val word = new Text();

    def map(key: LongWritable, value: Text,
            output: OutputCollector[Text, IntWritable],
            reporter: Reporter
           ) {
        val line = value.toString
        val tokenizer = new StringTokenizer(line)
        while (tokenizer.hasMoreTokens) {
            word.set(tokenizer.nextToken)
            output.collect(word, one)
        }
    }
}

class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    val logger = Logger.getLogger(this.getClass)

    def reduce(key: Text, values: Iterator[IntWritable],
               output: OutputCollector[Text, IntWritable],
               reporter: Reporter
              ) {
        output.collect(key, new IntWritable(count(0, values)))

        def count(sum: Int, vs: Iterator[IntWritable]): Int =
            if (vs.hasNext)
                count(sum + vs.next.get, vs)
            else
                sum
    }
}

class WordCount(username:String, groupname:String) {
    val logger = Logger.getLogger(this.getClass)

    val conf = new Configuration()
    conf.addResource("core-site.xml")
    conf.addResource("hdfs-site.xml")
    System.setProperty("HADOOP_USER_NAME",username)
    System.setProperty("HADOOP_GROUP_NAME", groupname)

    def count(args: Array[String]) = {
        val jobConf = new JobConf(conf, this.getClass)
        jobConf.setJobName("WordCount")
        jobConf.setOutputKeyClass(classOf[Text])
        jobConf.setOutputValueClass(classOf[IntWritable])

        jobConf.setMapperClass(classOf[Map])
        jobConf.setCombinerClass(classOf[Reduce])
        jobConf.setReducerClass(classOf[Reduce])
        jobConf.setInputFormat(classOf[TextInputFormat])
        jobConf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

        FileInputFormat.setInputPaths(jobConf, new Path(args(0)))
        FileOutputFormat.setOutputPath(jobConf, new Path(args(1)))

        JobClient.runJob(jobConf)
    }
}

object WordCount {
    def main(args: Array[String]) {
        val wordCount = new WordCount("root", "supergroup")
        wordCount.count(args)
    }
}