package com.tencent.tbds.demo.hbase

import com.tencent.tbds.demo.spark.SparkHBaseDemoOption
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 调用saveAsNewAPIHadoopDataset往HBase写数据，底层是批量、异步的方式，容易对region server造成太大压力，
 * 适用于中等规模数据的场景
 *
 * usage:
 * 1. 环境变量设置访问HDFS的认证参数
 *
 * 2. 提交spark任务：
 * spark-submit --class com.tencent.tbds.demo.spark.SparkWriteHBaseBatchDemo
 * --jars $(echo /usr/hdp/2.2.0.0-2041/hbase/lib/\*.jar | tr ' ' ',')
 * dev-demo-<version>.jar
 * --auth-id <auth id> --auth-key <auth key> --zk-host <host1,host2...> --table-name <tableName>
 *
 */
object SparkWriteHBaseBatchDemo {
  def main(args: Array[String]): Unit = {
    val option = new SparkHBaseDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkWriteHBaseDirectDemo")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, option.getTableName)
    conf.set("hbase.zookeeper.quorum", option.getZkHost)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    // 认证参数
    conf.set("hbase.security.authentication.tbds.secureid", option.getAuthId)
    conf.set("hbase.security.authentication.tbds.securekey", option.getAuthKey)

    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    createTable(conf)

    sc.makeRDD(
      Seq(("10001", "page#10001", 30), ("10002", "page#10002", 50))
    ).map(t => {
      val put = new Put(Bytes.toBytes(t._1))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes(t._2))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"), Bytes.toBytes(t._3))
      (new ImmutableBytesWritable(), put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def createTable(config: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin

    val tableName = TableName.valueOf(config.get(TableOutputFormat.OUTPUT_TABLE))

    if (!admin.tableExists(tableName)) {
      val table = new HTableDescriptor(tableName)
      val columnFamily = new HColumnDescriptor("cf")
      table.addFamily(columnFamily)
      admin.createTable(table)
    }
  }

}
