package com.wang.hello.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
  * Created by wangji on 2016/3/21.
  */
object Hello {
    def main(args: Array[String]): Unit = {
        /*val conf = new SparkConf().setMaster("local").setAppName("My App")
        val sc = new SparkContext("local", "My App")
        sc.stop()*/

        println("Hello Spark!")
    }
}
