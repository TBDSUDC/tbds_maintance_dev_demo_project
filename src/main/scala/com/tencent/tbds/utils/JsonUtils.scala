package com.tencent.tbds.utils

import org.apache.kafka.common.TopicPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.util.control.NonFatal

object JsonUtils {

  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Read partition offsets from json string
   */
  def partitionOffsets(str: String): Map[Int, Long] = {
    try {
      Serialization.read[Map[Int, Long]](str)
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"0":23,"1":15}, got $str""")
    }
  }

  /**
   * Write partition offsets as json string
   * @param partitionOffsets
   * @return
   */
  def partitionOffsets(partitionOffsets: Map[Int, Long]): String = {
    Serialization.write(partitionOffsets)
  }

  def topicPartitionOffsets(str: String): Map[String, Map[Int, Long]] = {
    Serialization.read[Map[String, Map[Int, Long]]](str)
  }

  /**
   * Write per-TopicPartition offsets as json string
   *
   * e.g. """{"topic1":{"0":23,"1":15},"topic2":{"0":10}}"""
   */
  def topicPartitionOffsets(partitionOffsets: Map[TopicPartition, Long]): String = {
    val result = mutable.Map[String, mutable.Map[Int, Long]]()

    val partitions = partitionOffsets.keySet.toSeq.sortBy(x => (x.topic(), x.partition()))
    partitions.foreach { tp =>
      val off = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, mutable.Map[Int, Long]())
      parts += tp.partition -> off
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }

}
