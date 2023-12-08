package org.hackweek.xing.ProcessorAPI.processors

import org.apache.kafka.streams.processor.{Cancellable, PunctuationType}
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.state.KeyValueStore
import org.hackweek.xing.ProcessorAPI.model.{DigitalTwin, StateType, TurbineState}
import org.slf4j.LoggerFactory

import java.time.{Duration, Instant}
import scala.util.Try
import scala.collection.JavaConverters._

class DigitalTwinProcessor extends Processor[String, TurbineState, String, DigitalTwin] {

  private val log = LoggerFactory.getLogger(getClass)

  private var context: ProcessorContext[String, DigitalTwin] = null
  private var kvStore: KeyValueStore[String, DigitalTwin]    = null
  private var punctuator: Cancellable                        = null

  override def init(context: ProcessorContext[String, DigitalTwin]): Unit = {
    this.context = context

    log.info(s"[DigitalTwinProcessor] Metadata: ${context.recordMetadata()}")

    // retrieve KV store that we created in the topology
    this.kvStore = context.getStateStore("digital-twin-store").asInstanceOf[KeyValueStore[String, DigitalTwin]]

    // schedule a punctuator method every 5 minutes based on Wallclock time
    punctuator = this.context.schedule(Duration.ofMinutes(5), PunctuationType.WALL_CLOCK_TIME, this.enforce(_))
  }

  override def process(record: Record[String, TurbineState]): Unit = {

    val retrievedValue = kvStore.get(record.key())
    // either of the records can arrive at any time; so we store them in the kvStore and create the record partially
    // as and when either arrive
    val stateType = record.value().stateType

    val digitalTwin = if (retrievedValue == null) DigitalTwin(None, None) else retrievedValue

    stateType match {
      case StateType.DESIRED  => digitalTwin.desired = Some(record.value())
      case StateType.REPORTED => digitalTwin.reported = Some(record.value())
    }

    kvStore.put(record.key(), digitalTwin)

    val newRecord = new Record[String, DigitalTwin](record.key(), digitalTwin, record.timestamp())
    context.forward(newRecord)

  }

  override def close(): Unit = {}

  // Punctuator scheduled function to delete stale records; i.e. records older than > 7 days
  def enforce(timestamp: Long): Unit = {
    val entries = kvStore.all().asScala

    entries.foreach { entry =>
      log.info(s"Checking if record has expired/gone stale: ${entry.key}")
      val lastReportedState = entry.value.reported
      if (lastReportedState != null) {
        val lastUpdated          = Instant.parse(lastReportedState.get.timestamp)
        val daysSinceLastUpdated = Duration.between(lastUpdated, Instant.now()).toDays
        if (daysSinceLastUpdated >= 7) kvStore.delete(entry.key)
      }
    }
  }

}
