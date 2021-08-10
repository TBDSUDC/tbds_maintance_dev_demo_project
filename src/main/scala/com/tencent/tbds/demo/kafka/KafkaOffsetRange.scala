package com.tencent.tbds.demo.kafka

import org.apache.kafka.common.TopicPartition

case class KafkaOffsetRange(tp: TopicPartition, fromOffset: Long, untilOffset: Long)
