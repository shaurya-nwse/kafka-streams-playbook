package org.hackweek.xing.StatefulProcessing.serde

import org.hackweek.xing.StatefulProcessing.model.ScoreEvent
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import play.api.libs.json.{Json, OFormat, OWrites}

import scala.util.{Failure, Success, Try}

class ScoreEventSerde extends Serde[ScoreEvent] {

  override def serializer(): Serializer[ScoreEvent] = new ScoreEventSerializer

  override def deserializer(): Deserializer[ScoreEvent] = new ScoreEventDeserializer

}

class ScoreEventSerializer extends Serializer[ScoreEvent] {
  override def serialize(topic: String, data: ScoreEvent): Array[Byte] = {
    implicit val scoreEventWrites: OWrites[ScoreEvent] = Json.writes[ScoreEvent]

    Try(Json.toJson(data)) match {
      case Success(value) => Json.toBytes(value)
      case Failure(exception) =>
        println(s"Could not parse record into Array[Byte]. Error: ${exception.printStackTrace()}")
        null
    }
  }
}

class ScoreEventDeserializer extends Deserializer[ScoreEvent] {
  override def deserialize(topic: String, data: Array[Byte]): ScoreEvent = {
    implicit val scoreEventReads: OFormat[ScoreEvent] = Json.format[ScoreEvent]

    Try(Json.parse(data)) match {
      case Success(value) => value.as[ScoreEvent]
      case Failure(exception) =>
        println(s"Could not parse record into ScoreEvent. Error: ${exception.printStackTrace()}")
        null
    }
  }
}
