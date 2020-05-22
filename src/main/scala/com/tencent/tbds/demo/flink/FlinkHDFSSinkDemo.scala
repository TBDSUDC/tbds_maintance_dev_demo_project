package com.tencent.tbds.demo.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
 * 运行环境设置：
 * 后台登录oceanus服务器，修改flink-conf.yaml配置文件，该文件位于/usr/hdp/2.2.0.0-2041/oceanus/oceanus-1.0.0/conf/flink-conf.yaml,
 * 设置如下配置项：
 *
 * state.backend: filesystem
 * state.backend.fs.checkpointdir: hdfs://hdfsCluster/flink-checkpoints
 *
 * backend可以设置为filesystem或rocksdb
 * checkpointdir设置为HDFS的某个路径
 *
 */
object FlinkHDFSSinkDemo {
  def main(args: Array[String]): Unit = {
    val option = new FlinkHDFSSinkDemoOption(args)
    if (option.hasHelp) {
      option.printHelp()
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60 * 1000)

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

    // StreamingFileSink实现方式
    val sink = StreamingFileSink
      .forRowFormat(new Path(option.getHdfsPath), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(DefaultRollingPolicy.create().build())
      .build()

    // BucketingSink实现方式
    //    val sink = new BucketingSink[String](option.getHdfsPath)
    //      .setBatchSize(128 * 1024 * 1024)
    //      .setBatchRolloverInterval(2 * 60 * 1000)
    //      .setInactiveBucketCheckInterval(60 * 1000)
    //      .setInactiveBucketThreshold(60 * 1000)

    stream.addSink(sink)

    env.execute("FlinkHDFSSinkDemo")
  }
}
