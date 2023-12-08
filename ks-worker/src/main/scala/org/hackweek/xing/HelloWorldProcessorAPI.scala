package org.hackweek.xing

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.util.Properties

object HelloWorldProcessorAPI extends App {

  val topology = new Topology

  val helloProcessor = new ProcessorSupplier[Void, String, Void, Void] {
    override def get(): Processor[Void, String, Void, Void] = new SayHelloProcessor
  }

  topology.addSource("UserSource", "users")
  topology.addProcessor("HelloProcessor", helloProcessor, "UserSource")

  val properties = new Properties
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "develop2")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Void().getClass)
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

  val streams = new KafkaStreams(topology, properties)
  streams.start()

  sys.ShutdownHookThread {
    streams.close()
  }

}
