package com.tencent.tbds.demo.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Spark Streaming消费kafka
 *
 * usage:
 * spark-submit --class com.tencent.tbds.demo.spark.SparkStreamKafkaDemo
 * --master yarn
 * --deploy-mode client
 * dev-demo-<version>.jar
 * --kafka-brokers <kafka brokers> --auth-id <auth id> --auth-key <auth key>
 * --topic <topic name>
 *
 */
object SparkStreamKafkaDemo {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val option = new SparkStreamKafkaDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    val sparkConf = new SparkConf().setAppName("SparkStreamKafkaDemo")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5000")

    val ssc = new StreamingContext(sparkConf, Seconds(option.getBatchDuration))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> option.getKafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> option.getGroupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "3000",
      "security.protocol" -> "SASL_TBDS",
      "sasl.mechanism" -> "TBDS",
      "sasl.tbds.secure.id" -> option.getAuthId,
      "sasl.tbds.secure.key" -> option.getAuthKey
    )

    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq(option.getTopic), kafkaParams)
    )

    dStream.foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        logger.info("================== no streaming data found ==================")
      } else {
        rdd.foreach(record => logger.info("===================>>>>>>" + record.value()))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
