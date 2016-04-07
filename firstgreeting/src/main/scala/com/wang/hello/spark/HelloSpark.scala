package com.wang.hello.spark

/**
  * Created by root on 4/6/16.
  */

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf

object HelloSpark {
    final val appName: String = "Hello Spark"

    def main(args: Array[String]): Unit = {
        val spark = new HelloSpark(args(0))
    }
}

class HelloSpark(val master: String) {
    val conf = new SparkConf().setAppName(HelloSpark.appName).setMaster(master);
    val sc = new JavaSparkContext(conf);
}
