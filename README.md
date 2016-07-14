com.wang.hello.hadoop.HdfsClient
  A simple HDFS client

com.wang.hello.hadoop.mapred.WordCount
  A map reduce example. Is able to be run under Hadoop or Yarn

com.wang.hello.spark.HelloSpark
  a Spark based map reduce, is able to be run under local or standalone Spark, or Yarn(both client or cluster mode) on Spark
  This example retreive data from HDFS, and write the result to HDFS also.

com.wang.hello.spark.HelloStreaming
  Almost same to HelloSpark, but receive data from socket streaming.
  By default, this example will acts as a client to read data from socket port 9999, you can start a test server by the following command:

    $ nc -lk 9999

  This will create a socket server on port 9999. When program connected, typing in words separated by comma(,), it will be received by
  the program.