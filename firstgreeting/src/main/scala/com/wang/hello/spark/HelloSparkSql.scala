package com.wang.hello.spark

/**
  * Created by root on 4/12/16.
  */
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}


// define the schema. Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
case class Person(name: String, age: Int)

object HelloSparkSql{
    val jsonDataSet = """{"name":"Yin", "age":20, "address":{"city":"Columbus","state":"Ohio"}}"""
    val jsonDataFile = "hdfs://hadoop:9000/user/root/spark/hello/people.json"

    val querySql = "SELECT name, age, address FROM people WHERE age >= 13 AND age < 30"
}

class HelloSparkSql(val user: String, val master: String) {
    var logger = Logger.getLogger(this.getClass)

    val sc: SparkContext = new SparkContext(configure(user, master))
    val sqlContext = new SQLContext(sc)

    /**
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
      *
      */
    def print(df:DataFrame): Unit = {
        df.show()
        df.printSchema()
        df.select("name").show()
        df.select("address.state").show()
        df.select(df("name"), df("age") + 1).show()
        df.filter(df("age") > 21).show()
        df.groupBy("age").count().show()
    }

    /**
      * Some example for DataSet generation
      */
    def generateRDDExample(): Unit = {
        // this is used to implicitly convert an RDD to DataFrame
        import sqlContext.implicits._

        // Encoders for most common types are automatically provided by importing sqlContext.implicits._
        val ds = Seq(1, 2, 3).toDS()
        val dsArr = ds.map(_ + 1).collect() // Returns: Array(2, 3, 4)

        ds.show()
        ds.printSchema()
        println(dsArr)

        // Encoders are also created for case classes.
        val person = Seq(Person("Andy", 32)).toDS()
        person.show()
        person.printSchema()
    }

    /**
      *
      * @param path
      * @return
      */
    def loadDataRDDFromFile(path:String):DataFrame = {
        // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
        val people = sqlContext.read.format("json").json(path)
        people
    }

    /**
      * json == """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}"""
      *
      * @param json
      * @return
      */
    def loadDataRDDFromString(json:String): DataFrame = {
        // Alternatively, a DataFrame can be created for a JSON dataset represented by
        // an RDD[String] storing one JSON object per string.
        val anotherPeopleRDD = sc.parallelize(json :: Nil)
        val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
        anotherPeople
    }

    /**
      *
      * @param dataSet
      * @param sql
      */
    def query(dataSet:DataFrame, sql:String): DataFrame = {
        // The inferred schema can be visualized using the printSchema() method.
        dataSet.printSchema()
        // root
        //  |-- age: integer (nullable = true)
        //  |-- name: string (nullable = true)

        // Register this DataFrame as a table.
        dataSet.registerTempTable("people")

        // SQL statements can be run by using the sql methods provided by sqlContext.
        val teenagers = sqlContext.sql(sql)
        teenagers
    }

    def createTable(): Unit = {

    }
}
