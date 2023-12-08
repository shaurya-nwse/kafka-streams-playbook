package org.hackweek.xing.StatelessProcessing.serialization.json

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.hackweek.xing.StatelessProcessing.serialization.Tweet
import play.api.libs.json.{Json, OFormat, OWrites}

class TwitterSerde extends Serde[Tweet] {

  override def deserializer(): Deserializer[Tweet] = new TwitterDeserializer

  override def serializer(): Serializer[Tweet] = new TwitterSerializer
}

class TwitterSerializer extends Serializer[Tweet] {

  override def serialize(topic: String, data: Tweet): Array[Byte] = {
    implicit val tweetWrites: OWrites[Tweet] = Json.writes[Tweet]
    if (data == null) null
    else {
      val json = Json.toJson(data)
      Json.toBytes(json)
    }
  }
}

class TwitterDeserializer extends Deserializer[Tweet] {
  override def deserialize(topic: String, data: Array[Byte]): Tweet = {
    implicit val tweetFormat: OFormat[Tweet] = Json.format[Tweet]
    if (data == null) null
    else {
      val json = Json.parse(data)
      json.as[Tweet]
    }
  }
}
