package com.tencent.tbds.demo.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkWriteHiveTableDemo {
  def main(args: Array[String]): Unit = {
    val option = new WriteHiveTableDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    val spark = SparkSession.builder().appName("WriteHiveTableDemo")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.metastore.uris", option.getHiveMetastoreUris)
      .config("fs.defaultFS", option.getDefaultFS)
      .enableHiveSupport()
      .getOrCreate()

    // 根据具体的数据格式，调用相应的read API，如orc、parquet、json...
    // 调用DataFrame的insertInto函数，将数据输出到hive表

    // 使用DataFrame的insertInto往hive表写数据时，schema的顺序需要和hive表里面字段顺序一致

    val tableSchema = spark.table(s"${option.getHiveDb}.${option.getHiveTable}").schema

    spark.read.orc(option.getHdfsPath)
      .select(tableSchema.fields.map(f => col(f.name)): _*)
      .write.mode(SaveMode.Append)
      .format("orc")
      .insertInto(s"${option.getHiveDb}.${option.getHiveTable}")
  }
}
