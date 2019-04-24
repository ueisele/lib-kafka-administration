package org.apache.kafka.clients.admin

import java.lang.String.format

import joptsimple.ValueConverter
import org.apache.kafka.common.TopicPartition

class TopicPartitionValueConverter() extends ValueConverter[TopicPartition] {

  val pattern = """([a-zA-Z0-9_\-.]+)/(\d+)""".r("topic", "partition")

  override def convert(value: String): TopicPartition = {
    value match {
      case pattern(topic, partition) => new TopicPartition(topic, partition.toInt)
      case _                         => throw new IllegalArgumentException(format("%s is not in the expected format %s", value, pattern.regex))
    }
  }

  override def valueType(): Class[_ <: TopicPartition] = classOf[TopicPartition]

  override def valuePattern(): String = pattern.regex
}
