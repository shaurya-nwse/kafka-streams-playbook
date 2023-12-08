package org.hackweek.xing

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.common.serialization.{Serdes => KSerdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.util.Properties

object HelloWorldDSL extends App {

  val builder = new StreamsBuilder

  implicit val consumed: Consumed[Void, String] = Consumed.`with`(KSerdes.Void(), Serdes.stringSerde)
  val stream                                    = builder.stream[Void, String]("users")

  stream.foreach { (k, v) =>
    println(s"Hello $v")
  }

  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "develop1")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KSerdes.Void().getClass)
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

  val streams = new KafkaStreams(builder.build(), properties)
  streams.start()

  sys.ShutdownHookThread {
    streams.close()
  }

}
