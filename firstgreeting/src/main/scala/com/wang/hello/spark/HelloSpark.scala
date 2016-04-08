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
        val spark = if(args.length == 2) {
            new HelloSpark(args(0), args(1))
        }
        else  if(args.length == 1) {
            new HelloSpark(args(0))
        }
        else {
            new HelloSpark()
        }
    }
}

class HelloSpark(val user:String, val master: String) {
    def this() = this(null, null)
    def this(user:String) = this(user, null)

    var logger = Logger.getLogger(this.getClass)

    val conf = new SparkConf().setAppName(HelloSpark.appName)

    if(user != null) {
        conf.set("spark.ui.view.acls", user)
        conf.set("spark.modify.acls", user)
    }

    if(null != master) {
        logger.debug("Master URL: " +  master)
        conf.setMaster(master)
    }
    else {
        logger.debug("No master URL has been specified")
    }

    val sc = new SparkContext(conf)
}
