package org.hackweek.xing.StatelessProcessing.serialization.json

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.charset.StandardCharsets

object TweetSerde {

  implicit def serde[A >: Null: Encoder: Decoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
    val deserializer = (ab: Array[Byte]) => {
      val string = new String(ab)
      decode[A](string).toOption
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

}
