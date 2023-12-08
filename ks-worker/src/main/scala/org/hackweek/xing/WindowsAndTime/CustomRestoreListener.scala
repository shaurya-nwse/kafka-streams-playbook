package org.hackweek.xing.WindowsAndTime

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.processor.StateRestoreListener
import org.slf4j.LoggerFactory

class CustomRestoreListener extends StateRestoreListener {

  private val log = LoggerFactory.getLogger(getClass)

  override def onRestoreStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long, endingOffset: Long): Unit = {
    log.info(s"The following state store is being restored: $storeName")
  }

  override def onRestoreEnd(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
    log.info(s"Restore complete for store: $storeName, for topic-partition: $topicPartition")
  }

  override def onBatchRestored(topicPartition: TopicPartition, storeName: String, batchEndOffset: Long, numRestored: Long): Unit = {}

}
