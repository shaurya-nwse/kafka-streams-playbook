package org.hackweek.xing.WindowsAndTime.serde

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import play.api.libs.json.{Json, OFormat, OWrites}

import scala.util.{Failure, Success, Try}

object PatientEventsSerdes {
  implicit def patientEventsSerde[A >: Null](implicit writes: OWrites[A], reads: OFormat[A]): Serde[A] = {
    new PatientEventsSerde[A]
  }

  class PatientEventsSerde[A >: Null](implicit writes: OWrites[A], reads: OFormat[A]) extends Serde[A] {

    override def serializer(): Serializer[A] = new PatientEventsSerializer[A]

    override def deserializer(): Deserializer[A] = new PatientEventsDeserializer[A]

  }

  class PatientEventsSerializer[A >: Null](implicit writes: OWrites[A]) extends Serializer[A] {

    override def serialize(topic: String, data: A): Array[Byte] = {

      Try(Json.toJson(data)) match {
        case Success(value) => Json.toBytes(value)
        case Failure(exception) =>
          println(s"Could not parse record into Array[Byte]. Error: ${exception.printStackTrace()}")
          null
      }
    }
  }

  class PatientEventsDeserializer[A >: Null](implicit reads: OFormat[A]) extends Deserializer[A] {
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
