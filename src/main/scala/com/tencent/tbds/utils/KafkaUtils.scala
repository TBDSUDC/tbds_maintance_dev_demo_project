package com.tencent.tbds.utils

import com.tencent.tbds.demo.kafka.KafkaOffsetRange
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.util.{Properties, UUID}
import scala.collection.JavaConverters._

object KafkaUtils {

  /**
    * 从kafka中获取Topic当前每个分区的beginningOffset和endOffset
    * @param topics
    * @return
    */
  def fetchOffsetRanges(topics: String): Set[KafkaOffsetRange] = {

    val uniqueGroupId = s"fetch-offset-${UUID.randomUUID}"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    props.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    props.put("group.id", uniqueGroupId)
    props.put("max.poll.records", "1")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(topics.split(",").map(_.trim()).filter(_.nonEmpty).toSeq.asJava)

    consumer.poll(1000)

    val partitions = consumer.assignment

    val beginningOffsets = consumer.beginningOffsets(partitions)

    val endOffsets = consumer.endOffsets(partitions)

    consumer.close()

    partitions.asScala.map(tp =>
      KafkaOffsetRange(tp, beginningOffsets.get(tp), endOffsets.get(tp))
    ).toSet

  }

}
