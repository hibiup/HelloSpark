package com.wang.hello.hadoop
/**
  * Created by wangji on 2016/3/23.
  */
import org.scalatest._, org.scalatest.junit._;
import org.junit._, runner.RunWith, Assert._

@RunWith(classOf[JUnitRunner])
class HadoopTest extends FunSuite{
    import org.apache.hadoop.conf.Configuration
    test("config") {
        val conf = new Configuration()
        conf .addResource("configure-1.xml")
        conf .addResource("configure-2.xml")
        println(conf.get("size-weight"))
    }

    test("list files") {
        val client:HdfsClient = new HdfsClient("root")
        val files = client.ls("/user/root/")
        println(files)
    }

    test("make dir") {
        val client:HdfsClient = new HdfsClient("root")
        client.mkdir("/temp")
    }

    test("remote dir") {
        val client:HdfsClient = new HdfsClient("root")
        client.rd("/temp")
    }
}
