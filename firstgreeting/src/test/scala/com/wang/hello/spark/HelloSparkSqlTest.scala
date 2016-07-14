package com.wang.hello.spark

import org.apache.log4j.Logger
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

/**
  * Created by root on 4/12/16.
  */
@RunWith(classOf[JUnitRunner])
class HelloSparkSqlTest extends FunSpec {
    var logger = Logger.getLogger(this.getClass)
    var master = "local[2]"
    var jsonDataFile = "hdfs://hadoop:9000/user/root/spark/hello/people.json"

    val sparkSql = new HelloSparkSql("root", master)

    describe("test show json dataset") {
        sparkSql.print(sparkSql.loadDataRDDFromFile(jsonDataFile))
    }

    describe("test generate rdd") {
        sparkSql.generateRDDExample()
    }

    describe("test query") {
        sparkSql.print(
            sparkSql.query(
                sparkSql.loadDataRDDFromString(HelloSparkSql.jsonDataSet),
                HelloSparkSql.querySql)
        )
    }
}
