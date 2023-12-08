package org.hackweek.xing

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util.Properties

object TestKTable extends App {

  val builder = new StreamsBuilder

  type Profile = String
  type UserId  = String
  case class Discount(profile: Profile, amount: Double)

  implicit def serdeOrder[A >: Null: Encoder: Decoder]: Serde[A] = {
    val serializer = (d: A) => d.asJson.noSpaces.getBytes()
    val deserializer = (a: Array[Byte]) => {
      val string = new String(a)
      decode[A](string).toOption
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  val discountsByUserTable = builder.table[UserId, Discount]("discount-profiles-by-user")

  val discountsStream = discountsByUserTable.toStream((userId, discount) => userId)

  discountsStream.to("discounts-ktable-test")

  val topology = builder.build()
  val props    = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-ktable-1")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

  //    println(topology.describe())

  val application = new KafkaStreams(topology, props)
  application.start()

}
