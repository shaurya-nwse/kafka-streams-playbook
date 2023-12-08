package org.hackweek.xing.StatefulProcessing.serde

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.hackweek.xing.StatefulProcessing.model.{DataModels, JoinDataModels}
import play.api.libs.json.{Json, OFormat, OWrites}

import scala.util.{Failure, Success, Try}

object VideoGameSerde {

  implicit def videoGameSerde[A >: Null](implicit writes: OWrites[A], reads: OFormat[A]): Serde[A] = {
    new VideoGameSerde[A]
  }

  class VideoGameSerde[A >: Null](implicit writes: OWrites[A], reads: OFormat[A]) extends Serde[A] {

    override def serializer(): Serializer[A] = new VideoGameSerializer[A]

    override def deserializer(): Deserializer[A] = new VideoGameDeserializer[A]

  }

  class VideoGameSerializer[A >: Null](implicit writes: OWrites[A]) extends Serializer[A] {

    override def serialize(topic: String, data: A): Array[Byte] = {

      Try(Json.toJson(data)) match {
        case Success(value) => Json.toBytes(value)
        case Failure(exception) =>
          println(s"Could not parse record into Array[Byte]. Error: ${exception.printStackTrace()}")
          null
      }
    }
  }

  class VideoGameDeserializer[A >: Null](implicit reads: OFormat[A]) extends Deserializer[A] {
    override def deserialize(topic: String, data: Array[Byte]): A = {

      Try(Json.parse(data)) match {
        case Success(value) => value.as[A]
        case Failure(exception) =>
          println(s"Could not parse record into DataModel. Error: ${exception.printStackTrace()}")
          null
      }
    }
  }

}
