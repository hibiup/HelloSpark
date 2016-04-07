package com.wang.hello.spark

/**
  * Created by root on 4/6/16.
  */

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.Logger

object HelloSpark {
    val appName: String = "Hello Spark"

    def main(args: Array[String]): Unit = {
        if(args.length >= 1) {
            val spark = new HelloSpark(args(0))
        }
        else {
            val spark = new HelloSpark()
        }
    }
}

class HelloSpark(val master: String) {
    var logger = Logger.getLogger(this.getClass)
    def this() = this(null)

    val conf = new SparkConf().setAppName(HelloSpark.appName)
    if(null != master) {
        logger.debug("Master URL: " +  master)
        conf.setMaster(master)
    }
    else
        logger.debug("No master URL has been specified")

    val sc = new SparkContext(conf)
}
