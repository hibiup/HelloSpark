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
        val wordCount = new WordCount("root", "supergroup")
        wordCount.count(Array("/user/root/host.access.shop.log-sample", "/user/root/count"))
    }
}
