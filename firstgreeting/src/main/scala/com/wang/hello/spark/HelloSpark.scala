package com.wang.hello.spark

/**
  * Created by root on 4/6/16.
  */

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.Logger

object HelloSpark {
    val appName: String = "Hello Spark"

    def main(args: Array[String]): Unit = {
        val spark = if (args.length == 2) {
            new HelloSpark(args(0), args(1))
        }
        else if (args.length == 1) {
            new HelloSpark(args(0))
        }
        else {
            new HelloSpark()
        }
    }
}

class HelloSpark(val user: String, val master: String) {
    def this() = this(null, null)

    def this(user: String) = this(user, null)

    var logger = Logger.getLogger(this.getClass)
    val conf = new SparkConf().setAppName(HelloSpark.appName)
    conf.setJars(List[String](this.getClass.getName))

    if (user != null) {
        conf.set("spark.ui.view.acls", user)
        conf.set("spark.modify.acls", user)
    }

    if (null != master) {
        logger.debug("Master URL: " + master)
        conf.setMaster(master)
    }
    else {
        logger.debug("No master URL has been specified")
    }

    val sc = new SparkContext(conf)

    /**
      * map reduce
      */
    def wordCount(path: String): Any = {
        /**
          * Open file from HDFS
          */
        val file = sc.textFile(path)

        /**
          * map: 是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。
          * 举例：
          * scala> val a = sc.parallelize(1 to 9, 3)
          * scala> val b = a.map(x => x*2)
          * scala> a.collect
          * res10: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
          * scala> b.collect
          * res11: Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18)
          *
          * flatMap: 与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。
          * 举例：对原RDD中的每个元素x产生y个元素（从1到y，y为元素x的值）
          * scala> val a = sc.parallelize(1 to 4, 2)
          * scala> val b = a.flatMap(x => 1 to x)
          * scala> b.collect
          * res12: Array[Int] = Array(1, 1, 2, 1, 2, 3, 1, 2, 3, 4)
          *
          * reduceByKey: 顾名思义，reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
          * 举例:
          * scala> val a = sc.parallelize(List((1,2),(3,4),(3,6)))
          * scala> a.reduceByKey((x,y) => x + y).collect
          * res7: Array[(Int, Int)] = Array((1,2), (3,10))
          *
          * 更多参考： https://www.zybuluo.com/jewes/note/35032
          */
        val count = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
        try{
            count.collect()
        }
        catch {
            case e:Exception => logger.debug(e.getMessage(), e)
        }
    }
}
