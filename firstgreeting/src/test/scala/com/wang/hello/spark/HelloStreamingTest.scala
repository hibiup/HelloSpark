package com.wang.hello.spark

import org.apache.log4j.Logger
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

/**
  * Created by root on 4/11/16.
  */
@RunWith(classOf[JUnitRunner])
class HelloStreamingTest extends FunSpec {
    var logger = Logger.getLogger(this.getClass)
    var master = "spark://hadoop:7077"

    describe("hello spark streaming") {
        val helloStreaming = new HelloStreaming("root", master)
        helloStreaming.wordCount((wordCounts) => {
            wordCounts.print()
        })
    }
}
