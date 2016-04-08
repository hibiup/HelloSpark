package com.wang.hello.spark

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import org.scalatest.junit._
import org.junit._
import runner.RunWith
import Assert._
import org.apache.log4j.Logger

import org.apache.spark.rdd.RDD

/**
  * Created by root on 4/6/16.
  */
@RunWith(classOf[JUnitRunner])
class HelloSparkTest extends FunSpec {
    var logger = Logger.getLogger(this.getClass)

    describe("hello spark") {
        val helloSpark = new HelloSpark("root", "local")
        try {
            val res = helloSpark.wordCount("hdfs://hadoop:9000/user/root/host.access.shop.log-sample")
            res match {
                case a:Array[(String, Int)] =>  a.foreach((x) => println(x._1 + "\t" + x._2))
                case _ => println("Nothing")
            }
        }
        catch {
            case e:Exception => logger.debug(e.getMessage(), e)
        }
    }
}
