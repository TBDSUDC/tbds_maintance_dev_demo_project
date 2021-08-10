package com.tencent.tbds.demo.kafka

import com.tencent.tbds.utils.{JsonUtils, KafkaUtils, ZKClient}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

object KafkaBatchDemo {

  def main(args: Array[String]): Unit = {

    val topic = "test"

    val zkConnectString = "localhost:2181"

    // 起始offset，格式为json string
    val startOffsets = getStartOffsets(zkConnectString, topic)

    // 将起始offset转换成Map对象
    val startPartitionOffsets = startOffsets.map(str =>
      JsonUtils.partitionOffsets(str).map({ case (part, offset) => new TopicPartition(topic, part) -> offset })
    )

    // 当前每个分区的beginningOffset和endOffset
    val offsetRanges = KafkaUtils.fetchOffsetRanges(topic)

    // 批处理读取的startingOffsets，需要处理offset新增和过期的情况
    val fromOffsets = startPartitionOffsets match {
      case Some(spo) =>
        offsetRanges.map(o => {
          // 新增offset处理
          val off = spo.getOrElse(o.tp, o.fromOffset)

          // 过期offset处理
          val from = if (off < o.fromOffset) o.fromOffset else off

          o.tp -> from
        }).toMap
      case _ =>
        offsetRanges.map(o => o.tp -> o.fromOffset).toMap
    }

    // 批处理读取的endingOffsets
    val untilOffsets = offsetRanges.map(o => o.tp -> o.untilOffset).toMap

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("KafkaBatchDemo")
      .getOrCreate()

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", JsonUtils.topicPartitionOffsets(fromOffsets))
      .option("endingOffsets", JsonUtils.topicPartitionOffsets(untilOffsets))
      .load()
      .select(col("topic"), col("partition"), col("value").cast(StringType), col("offset"))

    df.show(false)

    spark.close()

    // 批处理完成后，保存untilOffsets到zookeeper，作为下次批处理的起始offset
    saveOffsets(zkConnectString, untilOffsets)
  }

  /**
    * 从zookeeper中获取保存的offset，以此作为起始offset。
    * 格式为json，e.g """{"0":23,"1":15}"""
    *
    * @param zkConnectString
    * @param topic
    * @return
    */
  def getStartOffsets(zkConnectString: String, topic: String): Option[String] = {
    val zkClient = new ZKClient(zkConnectString)
    val path = s"/pomp/topics/$topic"
    var data = Option.empty[String]
    if (zkClient.pathExists(path)) {
      data = Option(zkClient.getData(path))
    }
    zkClient.close()
    data
  }


  /**
   * 保存offset到zookeeper，作为下次批处理的起始offset
   *
   * @param zkConnectString
   * @param partitionOffsets
   */
  def saveOffsets(zkConnectString: String, partitionOffsets: Map[TopicPartition, Long]): Unit = {
    val zkClient = new ZKClient(zkConnectString)

    val result = mutable.Map[String, mutable.Map[Int, Long]]()
    val tps = partitionOffsets.keySet.toSeq.sortBy(x => (x.topic(), x.partition()))
    tps.foreach(tp => {
      val offset = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, mutable.Map[Int, Long]())
      parts += tp.partition() -> offset
      result += tp.topic() -> parts
    })

    result.keySet.foreach(topic => {
      val path = s"/pomp/topics/$topic"
      if (!zkClient.pathExists(path)) {
        zkClient.create(path)
      }
      zkClient.setData(path, JsonUtils.partitionOffsets(result(topic).toMap))
    })

    zkClient.close()
  }

}
