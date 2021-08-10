package com.tencent.tbds.demo.kafka

import com.tencent.tbds.utils.{JsonUtils, KafkaUtils}
import kafka.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.StringType

import java.util.Properties

object KafkaStreamDemo {

  def main(args: Array[String]): Unit = {
    val topic = "test"
    val brokers = "localhost:9092"
    val groupId = "g1"

    val properties= new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    val client = AdminClient.create(properties)

    val groupOffsets = client.listGroupOffsets(groupId)

    val offsetRanges = KafkaUtils.fetchOffsetRanges(topic)

    val fromOffsets = if (groupOffsets.isEmpty) {
      offsetRanges.map(o => o.tp -> o.fromOffset).toMap
    } else {
      offsetRanges.map(o => {
        // 新增offset处理
        val off = groupOffsets.getOrElse(o.tp, o.fromOffset)

        // 过期offset处理
        val from = if (off < o.fromOffset) o.fromOffset else off

        o.tp -> from
      }).toMap
    }


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("KafkaStreamDemo")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.sql.streaming.minBatchesToRetain", "50")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", JsonUtils.topicPartitionOffsets(fromOffsets))
      .load()
      .select(
        col("topic"),
        col("partition"),
        col("value").cast(StringType),
        col("offset"))
      //.withWatermark("ts", "5 seconds")
      //.groupBy(window(col("ts"), "5 seconds"), col("value"))
      //.count()
      //.select(col("value"))

    val query = df.writeStream
      //.queryName("stream-test")
      //.format("kafka")
      .format("console")
      .option("truncate", "false")
      //.option("kafka.bootstrap.servers", brokers)
      //.option("topic", "stream")
      .option("checkpointLocation", "/tmp/kafka-checkpoints/kafka-stream-demo")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    spark.streams.addListener(new KafkaOffsetCommitterListener(brokers, groupId))
    query.awaitTermination()

    //    停止流计算程序方式一：
    //    启动一个线程监听停止的条件
    //    object QueryManager {
    //      def queryTerminator(query: StreamingQuery): Runnable = new Runnable {
    //        override def run() = {if(stop condition) query.stop()}
    //      }
    //      def listenTermination(query: StreamingQuery) = {
    //        Executors.newSingleThreadScheduledExecutor.scheduleWithFixedDelay(
    //          queryTerminator(query), initialDelay=1, delay=1, SECONDS
    //        )
    //      }
    //    }
    //
    //    调用: QueryManager.listenTermination(query)

    //    停止流计算程序方式二：
    //    从某个地方(zookeeper, hdfs)中获取是否停止的状态
    //    while (!spark.sparkContext.isStopped) {
    //    val state = ...
    //      if (state = 0) {
    //        query.stop()
    //        spark.stop()
    //      } else {
    //        每分钟检查一下
    //        query.awaitTermination(60000)
    //      }
    //    }

  }

}

class KafkaOffsetCommitterListener(brokers: String, groupId: String) extends StreamingQueryListener {
  val properties= new Properties()
  properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](properties)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println("query started.........")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    //event.progress.sources.foreach(s => println(s.prettyJson))

    val topicPartitionOffsets = new java.util.HashMap[TopicPartition, OffsetAndMetadata]()

    event.progress.sources.foreach(source => {
      val parts = JsonUtils.topicPartitionOffsets(source.endOffset)

      for((topic, endOffsets) <- parts) {

        for((p, off) <- endOffsets) {
          val topicPartition = new TopicPartition(topic, p)
          val offsetAndMetadata = new OffsetAndMetadata(off)
          topicPartitionOffsets.put(topicPartition, offsetAndMetadata)
        }

      }
    })

    consumer.commitSync(topicPartitionOffsets)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println("query terminated.........")
    consumer.close()
  }
}
