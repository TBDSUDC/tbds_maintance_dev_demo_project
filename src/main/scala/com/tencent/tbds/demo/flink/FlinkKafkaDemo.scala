package com.tencent.tbds.demo.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object FlinkKafkaDemo {
  def main(args: Array[String]): Unit = {
    val option = new FlinkKafkaDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", option.getKafkaBrokers)
    props.put("group.id", option.getGroupId)
    props.put("auto.offset.reset", "latest") // latest/earliest
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "3000")

    // TBDS原生认证参数设置
    props.put("security.protocol", "SASL_TBDS")
    props.put("sasl.mechanism", "TBDS")
    props.put("sasl.tbds.secure.id", option.getAuthId)
    props.put("sasl.tbds.secure.key", option.getAuthKey)

    val consumer = new FlinkKafkaConsumer010[String](option.getTopic, new SimpleStringSchema(), props)

    val stream = env.addSource(consumer)
    stream.print()

    env.execute("FlinkKafkaDemo")
  }
}
