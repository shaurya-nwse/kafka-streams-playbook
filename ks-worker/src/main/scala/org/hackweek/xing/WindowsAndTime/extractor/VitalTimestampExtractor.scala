package org.hackweek.xing.WindowsAndTime.extractor

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import org.hackweek.xing.WindowsAndTime.model.Vital

import java.time.Instant

class VitalTimestampExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], partitionTime: Long): Long = {

    val measurement = record.value().asInstanceOf[Vital]

    if (measurement != null && measurement.getTimestamp != null) {
      val timestamp = measurement.getTimestamp
      Instant.parse(timestamp).toEpochMilli
    } else {
      partitionTime
    }
  }

}
