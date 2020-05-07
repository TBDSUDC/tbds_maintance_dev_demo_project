package com.tencent.tbds.demo.spark

import org.apache.spark.sql.SparkSession

object SparkReadHiveTableDemo {
  def main(args: Array[String]): Unit = {
    val option = new ReadHiveTableDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    val spark = SparkSession.builder().appName("ReadHiveTableDemo")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.metastore.uris", option.getHiveMetastoreUris)
      .config("fs.defaultFS", option.getDefaultFS)
      .enableHiveSupport()
      .getOrCreate()

    spark.table(s"${option.getHiveDb}.${option.getHiveTable}").show()
  }
}
