package org.hackweek.xing.StatefulProcessing.serde

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.hackweek.xing.StatefulProcessing.aggregators.HighScores

import java.nio.charset.StandardCharsets
import java.util

class HighScoresSerde extends Serde[HighScores] {

  override def serializer(): Serializer[HighScores] = new HighScoresSerializer

  override def deserializer(): Deserializer[HighScores] = new HighScoresDeserializer

}

class HighScoresSerializer extends Serializer[HighScores] {

  private val gson = new GsonBuilder().create()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: HighScores): Array[Byte] = {
    gson.toJson(data).getBytes(StandardCharsets.UTF_8)
  }

  override def close(): Unit = {}

}

class HighScoresDeserializer extends Deserializer[HighScores] {

  private val gson = new GsonBuilder().create()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): HighScores = {
    if (data == null) null
    else {
      gson.fromJson(new String(data, StandardCharsets.UTF_8), classOf[HighScores])
    }
  }

  override def close(): Unit = {}
}
