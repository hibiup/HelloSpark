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
        conf.addResource("configure-1.xml")
        conf.addResource("configure-2.xml")
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

    test("read dir") {
        val client:HdfsClient = new HdfsClient("root")
        val in = client.read("/user/root/host.access.shop.log-sample")

        var b = new Array[Byte](1024)
        var numBytes = in.read(b)
        while (numBytes > 0) {
            println(new String(b, "UTF-8"))
            numBytes = in.read(b)
        }
    }
}
