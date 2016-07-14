package com.wang.hello.spark

/**
  * Created by root on 4/11/16.
  */

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object HelloStreaming {
    val propConf = ConfigFactory.load("spark.properties")
    val separator = propConf.getString("spark.streaming.separator")
    val interval = propConf.getInt("spark.streaming.socket.interval")
    val hostname = propConf.getString("spark.streaming.socket.hostname")
    val port = propConf.getInt("spark.streaming.socket.port")
    //var jars = Array(this.getClass.getProtectionDomain().getCodeSource().getLocation().toURI().getPath())

    def main(args: Array[String]): Unit = {
        val streaming:HelloStreaming = new HelloStreaming(args(0), args(1))

        streaming.wordCount((wordCounts) => {wordCounts.print()})
    }
}

class HelloStreaming(val user: String, val master: String) {
    def this() = this(null, null)

    def this(user: String) = this(user, null)

    var logger = Logger.getLogger(this.getClass)

    // Create Streaming context and batch interval of 1 second.
    val sc = new StreamingContext(configure(user, master), Seconds(HelloStreaming.interval))

    /**
      * Get Spark configuration
      *
      * @param user
      * @param master
      * @return
      */
    def configure(user: String, master: String): SparkConf = {
        val conf = new SparkConf().setAppName (HelloSpark.appName)
        if (user != null) {
            conf.set ("spark.ui.view.acls", user)
            conf.set ("spark.modify.acls", user)
        }

            if (null != master) {
            logger.debug ("Master URL: " + master)
            conf.setMaster (master)
        }
            else {
            logger.debug ("No master URL has been specified")
        }

        conf
    }

    /**
      * word count entry
      */
    def wordCount(resultHandler:(DStream[(String, Int)]) => Unit): Unit = {
        listen(HelloStreaming.hostname, HelloStreaming.port, receive, resultHandler)
    }

    /**
      * Listen the input
      *
      * @param hostname
      * @param port
      * @param receive
      * @param resultHandler
      */
    def listen(hostname:String,
               port:Int,
               receive:(ReceiverInputDStream[String]) => DStream[(String, Int)],
               resultHandler:(DStream[(String, Int)]) => Unit):Unit ={
        val lines = sc.socketTextStream(hostname, port)

        val wordCounts = receive(lines)
        //val wordCounts = lines.flatMap(_.split(HelloStreaming.separator)).map(word => (word, 1)).reduceByKey(_ + _)

        resultHandler(wordCounts)
        //wordCounts.print()

        sc.start()
        sc.awaitTermination()
    }

    /**
      * Process input
      *
      * @param input
      * @return
      */
    def receive(input: ReceiverInputDStream[String]): DStream[(String, Int)] = {
        input.flatMap(_.split(HelloStreaming.separator)).map(word => (word, 1)).reduceByKey(_ + _)
    }
}
