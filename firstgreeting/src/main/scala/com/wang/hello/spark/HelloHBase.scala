/**
  * Run with command:
  * $ /opt/spark/default/bin/spark-submit \
  *     --class com.wang.hello.spark.HBaseRead \
  *     --master yarn \
  *     --files /opt/hbase/default/conf/hbase-site.xml \
  *     --packages com.hortonworks:shc-core:1.1.0-2.1-s_2.11 \
  *     --repositories http://repo.hortonworks.com/content/groups/public/ \
  *     --num-executors 4 \
  *     --driver-memory 512m \
  *     --executor-memory 512m \
  *     --executor-cores 1 \
  *     firstgreeting-1.0.0-SNAPSHOT.jar
  */
package com.wang.hello.spark

import org.apache.spark._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog


object HelloHBase {
  // Defune table
  def catalog = s"""{
                    |"table":{"namespace":"default", "name":"table1"},
                    |"rowkey":"key",
                    |"columns":{
                    |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                    |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                    |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                    |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                    |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                    |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                    |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                    |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                    |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                    |}
                    |}""".stripMargin

  case class HBaseRecord(col0: String, col1: Boolean,col2: Double, col3: Float,col4: Int, col5: Long, col6: Short, col7: String, col8: Byte)

  val data = (0 to 255).map {
    i =>  HBaseRecord(i, "extra")
  }

  object HBaseRecord {
    def apply(i: Int, t: String): HBaseRecord = {
      val s = s"""row${"%03d".format(i)}"""
      HBaseRecord(s, i%2==0, i.toDouble, i.toFloat, i, i.toLong, i.toShort, s"String$i:$t", i.toByte)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Create table and write data
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    // Load data
    def withCatalog(cat: String) = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val df = withCatalog(catalog)

    // Filter data
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      ($"col4" === 1 ||
        $"col4" === 42))
      .select("col0", "col1", "col4")
    s.show

    // Execute SQL
    df.registerTempTable("table")
    sqlContext.sql("select count(col1) from table").show
  }
}