package org.hackweek.xing.StatelessProcessing.serialization.avro

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serde
import org.hackweek.xing.StatelessProcessing.EntitySentiment

import scala.collection.JavaConverters._

object AvroSerdes {

  def entitySentimentAvro(url: String, isKey: Boolean): Serde[EntitySentiment] = {
    val serde: Serde[EntitySentiment] = new SpecificAvroSerde[EntitySentiment]
    val serdeConfig                   = Map("schema.registry.url" -> url)
    serde.configure(serdeConfig.asJava, isKey)
    serde
  }

}
