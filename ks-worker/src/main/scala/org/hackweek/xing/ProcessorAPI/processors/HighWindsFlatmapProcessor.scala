package org.hackweek.xing.ProcessorAPI.processors

import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.hackweek.xing.ProcessorAPI.model.{Power, StateType, TurbineState}
import org.slf4j.LoggerFactory

class HighWindsFlatmapProcessor extends Processor[String, TurbineState, String, TurbineState] {

  private val log = LoggerFactory.getLogger(getClass)

  private var context: ProcessorContext[String, TurbineState] = null

  override def init(context: ProcessorContext[String, TurbineState]): Unit = {
    this.context = context
  }

  override def process(record: Record[String, TurbineState]): Unit = {
    val reported = record.value()
    // Context.forward, sends the record to the downstream processor (maybe another processor or a sink)
    context.forward(record)

    // check if winds are too high
    if (reported.windSpeedMph > 65 && reported.power == Power.ON) {
      log.info("High winds detected. Sending shutdown signal")
      val desired   = reported.copy(power = Power.OFF, stateType = StateType.DESIRED)
      val newRecord = new Record[String, TurbineState](record.key(), desired, record.timestamp())
      context.forward(newRecord)
    }
  }

  override def close(): Unit = {}

}
