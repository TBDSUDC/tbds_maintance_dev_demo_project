package com.tencent.tbds.demo.hbase

import com.tencent.tbds.demo.spark.SparkHBaseDemoOption
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 以HBase常规API的形式往HBase写数据，吞吐量低，适用于数据量较小的场景，如流计算。
 *
 * usage:
 * 1. 环境变量设置访问HDFS的认证参数
 *
 * 2. 提交spark任务：
 * spark-submit --class com.tencent.tbds.demo.spark.SparkWriteHBaseDirectDemo
 * --jars $(echo /usr/hdp/2.2.0.0-2041/hbase/lib/\*.jar | tr ' ' ',')
 * dev-demo-<version>.jar
 * --auth-id <auth id> --auth-key <auth key> --zk-host <host1,host2...> --table-name <tableName>
 *
 */
object SparkWriteHBaseDirectDemo {
  def main(args: Array[String]): Unit = {
    val option = new SparkHBaseDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    createTable(option)

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkWriteHBaseDirectDemo")
    val sc = new SparkContext(sparkConf)

    val zkHost = option.getZkHost
    val authId = option.getAuthId
    val authKey = option.getAuthKey
    val tableName = option.getTableName

    sc.makeRDD(
      Seq(("10003", "page#10003", 20), ("10004", "page#10004", 60))
    ).foreachPartition(iterator => {
      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      conf.set("hbase.zookeeper.quorum", zkHost)
      conf.set("zookeeper.znode.parent", "/hbase-unsecure")

      // 认证参数
      conf.set("hbase.security.authentication.tbds.secureid", authId)
      conf.set("hbase.security.authentication.tbds.securekey", authKey)

      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf(tableName))
      iterator.foreach(t => {
        val put = new Put(Bytes.toBytes(t._1))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes(t._2))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"), Bytes.toBytes(t._3))
        table.put(put)
      })
      table.close()
      connection.close()
    })
  }

  def createTable(option: SparkHBaseDemoOption): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", option.getZkHost)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    // 认证参数
    conf.set("hbase.security.authentication.tbds.secureid", option.getAuthId)
    conf.set("hbase.security.authentication.tbds.securekey", option.getAuthKey)

    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val tableName = TableName.valueOf(option.getTableName)

    if (!admin.tableExists(tableName)) {
      val table = new HTableDescriptor(tableName)
      val columnFamily = new HColumnDescriptor("cf")
      table.addFamily(columnFamily)
      admin.createTable(table)
    }
  }

}
