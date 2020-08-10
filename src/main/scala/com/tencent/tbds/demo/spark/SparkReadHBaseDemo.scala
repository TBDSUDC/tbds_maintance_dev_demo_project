package com.tencent.tbds.demo.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * usage:
 * 1. 环境变量设置访问HDFS的认证参数
 *
 * 2. 提交spark任务：
 * spark-submit --class com.tencent.tbds.demo.spark.SparkReadHBaseDemo
 * --jars $(echo /usr/hdp/2.2.0.0-2041/hbase/lib/\*.jar | tr ' ' ',')
 * dev-demo-<version>.jar
 * --auth-id <auth id> --auth-key <auth key> --zk-host <host1,host2...> --table-name <tableName>
 */
object SparkReadHBaseDemo {
  def main(args: Array[String]): Unit = {
    val option = new SparkHBaseDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkReadHBaseDemo")
    val sc = new SparkContext(sparkConf)

    // 设置过滤条件
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, option.getTableName)
    conf.set(TableInputFormat.SCAN, scanToString)

    conf.set("hbase.zookeeper.quorum", option.getZkHost)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    // 认证参数
    conf.set("hbase.security.authentication.tbds.secureid", option.getAuthId)
    conf.set("hbase.security.authentication.tbds.securekey", option.getAuthKey)

    sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    ).foreach(t => {
      val result = t._2
      println("====================row=========================>>" + Bytes.toString(result.getRow))
      println("====================cf:a=========================>>" + Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("a"))))
      println("====================cf:b=========================>>" + Bytes.toInt(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("b"))))
    })
  }
}
