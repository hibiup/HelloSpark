package com.wang.hello.spark

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest._, org.scalatest.junit._;
import org.junit._, runner.RunWith, Assert._

/**
  * Created by root on 4/6/16.
  */
@RunWith(classOf[JUnitRunner])
class HelloSparkTest extends FunSpec {
    describe("hello spark") {
        val helloSpark = new HelloSpark("spark://hadoop:7077")
        //println("Something!")
    }
}
