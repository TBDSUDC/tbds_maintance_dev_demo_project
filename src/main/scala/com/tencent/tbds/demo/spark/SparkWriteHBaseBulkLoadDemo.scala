package com.tencent.tbds.demo.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 生成HFile文件，再将此文件导入HBase，适用于大规模数据场景
 *
 * 需要注意的问题：
 * 1. hbase配置参数hbase.fs.tmp.dir指定的目录权限问题
 * 2. HFile输出路径访问权限问题
 *
 * usage:
 * 1. 环境变量设置访问HDFS的认证参数
 *
 * 2. 提交spark任务：
 * spark-submit --class com.tencent.tbds.demo.spark.SparkWriteHBaseBulkLoadDemo
 * --jars $(echo /usr/hdp/2.2.0.0-2041/hbase/lib/\*.jar | tr ' ' ',')
 * dev-demo-1.0-SNAPSHOT.jar
 * --auth-id <auth id> --auth-key <auth key> --auth-user <username> --zk-host <host1,host2...> --table-name <tableName>
 *
 */
object SparkWriteHBaseBulkLoadDemo {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val option = new SparkHBaseDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    if (option.getAuthUser == null) {
      logger.error("please specify --auth-user option")
      return
    }

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("SparkWriteHbaseBulkLoad")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("fs.defaultFS", "hdfs://hdfsCluster")
    conf.set(TableOutputFormat.OUTPUT_TABLE, option.getTableName)
    conf.set("hbase.zookeeper.quorum", option.getZkHost)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.fs.tmp.dir", s"/tmp/${option.getAuthUser}/hbase-staging") //默认值可能会有访问权限问题

    // 认证参数
    conf.set("hbase.security.authentication.tbds.secureid", option.getAuthId)
    conf.set("hbase.security.authentication.tbds.securekey", option.getAuthKey)

    createTable(conf)

    val tableName = TableName.valueOf("my_test")
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(tableName)
    val regionLocator = connection.getRegionLocator(tableName)

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    // 需要设置该路径访问权限
    val outputPath = s"hdfs://hdfsCluster/tmp/${option.getAuthUser}/hbase-hfile"
    val fs = FileSystem.get(conf)
    if (fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath), true)
    }

    sc.makeRDD(
      Seq(("10006", "page#10006", 60), ("10004", "page#10004", 40), ("10005", "page#10005", 50))
    ).sortBy(_._1).flatMap(t => {
      Seq(
        (new ImmutableBytesWritable(), new KeyValue(Bytes.toBytes(t._1), Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes(t._2))),
        (new ImmutableBytesWritable(), new KeyValue(Bytes.toBytes(t._1), Bytes.toBytes("cf"), Bytes.toBytes("b"), Bytes.toBytes(t._3)))
      )
    }).saveAsNewAPIHadoopFile(
      outputPath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf
    )

    val loader = new LoadIncrementalHFiles(conf)
    loader.doBulkLoad(new Path(outputPath), connection.getAdmin, table, regionLocator)

    table.close()
    connection.close()
    sc.stop()
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
