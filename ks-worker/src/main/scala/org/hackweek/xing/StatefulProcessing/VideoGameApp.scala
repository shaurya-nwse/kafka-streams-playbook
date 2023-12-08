package org.hackweek.xing.StatefulProcessing

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{Initializer, Printed}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.hackweek.xing.StatefulProcessing.model.{Enriched, Player, Product, ScoreEvent, ScoreWithPlayer}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, Materialized}
import org.apache.kafka.streams.state.KeyValueStore
import org.hackweek.xing.StatefulProcessing.aggregators.HighScores
import org.hackweek.xing.StatefulProcessing.serde.HighScoresSerde

import java.util.Properties

object VideoGameApp extends App {

  import org.hackweek.xing.StatefulProcessing.serde.VideoGameSerde._

  val builder                                 = new StreamsBuilder
  implicit val bytesSerde: Serde[Array[Byte]] = Serdes.byteArraySerde
  implicit val stringSerde: Serde[String]     = Serdes.stringSerde
  // High scores serde (from GSON)
  implicit val highScoresSerde: HighScoresSerde = new HighScoresSerde

  // 1. 3 Source processors
  val scoreEventsStream = builder
    .stream[Array[Byte], ScoreEvent]("score-events")
    .selectKey((k, v) => v.playerId.toString)
  val playersTable   = builder.table[String, Player]("players")
  val productsGTable = builder.globalTable[String, Product]("products")

  // 2. Join Score -> Player [KStream: KTable]
  // TODO: Check if Joined is provided implicitly; otherwise define implicit here
  val scoreWithPlayers = scoreEventsStream.join(playersTable) { (score, player) =>
    ScoreWithPlayer(score, player)
  }

  // 3. Join ScoreWithPlayer -> Product [KStream: GlobalKTable]
  val enrichedStream = scoreWithPlayers.join(productsGTable)(
    { case (playerId, scoreWithPlayer: ScoreWithPlayer) => scoreWithPlayer.scoreEvent.productId.toString },
    { case (scoreWithPlayer, product: Product)          => Enriched(scoreWithPlayer, product) }
  )
  // print to console
  enrichedStream.print(Printed.toSysOut[String, Enriched].withLabel("Enriched-with-products"))

  // 4. Group by Key (required before aggregating)
  val groupedStream: KGroupedStream[String, Enriched] = enrichedStream.groupBy((k, v) => v.productId.toString)

  // 5. Aggregate

  // Giving the materialized store a name; this is used when querying the state
  implicit val materialized: Materialized[String, HighScores, KeyValueStore[Byte, Array[Byte]]] = Materialized
    .as[String, HighScores, KeyValueStore[Byte, Array[Byte]]]("leader-boards")

  val highScores = groupedStream
    .aggregate(new HighScores)((k, v, agg) => agg.add(v))

  highScores.toStream.to("high-scores")

  val topology   = builder.build()
  val properties = new Properties
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-stateful-process")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
  properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7000")
  properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")

  val streams = new KafkaStreams(topology, properties)

  sys.ShutdownHookThread {
    streams.close()
  }

  streams.start()

  // TODO: Add querying state store with Play

}
