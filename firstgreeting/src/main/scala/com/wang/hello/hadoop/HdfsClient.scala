package com.wang.hello.hadoop

/**
  * Created by wangji on 2016/3/21.
  */
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

import java.io.File
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.InputStream

/**
  *
  * @param username
  * @param groupname
  */
class HdfsClient(val username:String, val groupname:String) {
    val logger = Logger.getLogger(this.getClass)
    /**
      * 基于 HADOOP 配置文件建立到达 HDFS 的连接
      */
    val conf = new Configuration()
    conf.addResource("core-site.xml")
    conf.addResource("hdfs-site.xml")
    System.setProperty("HADOOP_USER_NAME",username)
    System.setProperty("HADOOP_GROUP_NAME", groupname)

    // 获得 HDFS 文件系统句柄
    val fileSystem = FileSystem.get(conf)

    /**
      * 二次构造函数
      * @param username
      */
    def this(username:String)  {
        this(username, "supergroup")
    }

    def save(filepath: String): Unit = {
        val file = new File(filepath)
        val out = fileSystem.create(new Path(file.getName))
        val in = new BufferedInputStream(new FileInputStream(file))
        var b = new Array[Byte](1024)
        var numBytes = in.read(b)
        while (numBytes > 0) {
            out.write(b, 0, numBytes)
            numBytes = in.read(b)
        }
        in.close()
        out.close()
    }

    def read(filename: String): InputStream = {
        val path = new Path(filename)
        fileSystem.open(path)
    }

    def rd(filename: String): Boolean = {
        val path = new Path(filename)
        fileSystem.delete(path, true)
    }

    def mkdir(dir: String): Unit = {
        val path = new Path(dir)
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path)
        }
    }

    def ls(dir: String): Unit = {
        val path = new Path(dir)
        fileSystem.listStatus(path)
    }
}
