package com.tencent.tbds.demo.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

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
    conf.set(TableInputFormat.INPUT_TABLE, "my_test")
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
      println(Bytes.toString(result.getRow))
      println(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("a"))))
      println(Bytes.toInt(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("b"))))
    })
  }
}
