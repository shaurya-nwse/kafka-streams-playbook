package org.hackweek.xing

import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}

class SayHelloProcessor extends Processor[Void, String, Void, Void] {

  override def init(context: ProcessorContext[Void, Void]): Unit = super.init(context)

  override def process(record: Record[Void, String]): Unit = println(s"[Processor API] Hello, ${record.value()}")

  override def close(): Unit = super.close()

}
