package com.wang.hello.hadoop

/**
  * Created by wangji on 2016/3/24.
  */
import org.scalatest._, org.scalatest.junit._;
import org.junit._, runner.RunWith, Assert._

@RunWith(classOf[JUnitRunner])
class WordCountTest extends FunSuite {
    import com.wang.hello.hadoop.mapred.WordCount

    test("word count") {
        val user = "root"
        val userGroup = "supergroup";
        val outPath = "/user/root/count"
        val outFile = "part-r-00000"
        val inFile = "/user/root/host.access.shop.log-sample"

        val wordCount = new WordCount(user, userGroup)
        wordCount.count(Array(inFile, outPath))

        // Print result
        val client:HdfsClient = new HdfsClient(user)
        val in = client.read(outPath+ "/" + outFile)

        var b = new Array[Byte](1024)
        var numBytes = in.read(b)
        while (numBytes > 0) {
            println(new String(b, "UTF-8"))
            numBytes = in.read(b)
        }
    }
}
