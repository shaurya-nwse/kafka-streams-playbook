package org.hackweek.xing.StatelessProcessing

import io.circe.generic.auto._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.Printed
import org.hackweek.xing.StatelessProcessing.serialization.json.TweetSerde._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.hackweek.xing.StatelessProcessing.language.DummyClient
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.common.serialization.{Serdes => KSerdes}
import org.hackweek.xing.StatelessProcessing.serialization.Tweet
import org.hackweek.xing.StatelessProcessing.serialization.avro.AvroSerdes
import org.hackweek.xing.StatelessProcessing.serialization.json.TwitterSerde

import java.util.Properties

object TwitterSentimentApp extends App {

  private final val currencies = List("bitcoin", "ethereum")

  val languageClient = new DummyClient
  val builder        = new StreamsBuilder

  // 1. Start streaming tweets using custom value serdes
  // IMPLICIT serde from json TweetSerde
//  val stream = builder.stream[Array[Byte], Tweet]("tweets")

  implicit val consumed: Consumed[Array[Byte], Tweet] = Consumed.`with`(KSerdes.ByteArray(), new TwitterSerde())
  val stream                                          = builder.stream("tweets")

  // print Stream to stdout
  stream.print(Printed.toSysOut[Array[Byte], Tweet].withLabel("tweets-stream"))

  // 2. FILTER retweets
  val filtered = stream
    .filterNot((_, tweet) => tweet.retweet)

  // 3. BRANCH by language (english and non-english)
  val englishTweets    = (_: Array[Byte], tweet: Tweet) => tweet.lang.equals("en")
  val nonEnglishTweets = (_: Array[Byte], tweet: Tweet) => !tweet.lang.equals("en")
  val branches         = filtered.branch(englishTweets, nonEnglishTweets)

  // print english and nonEnglish tweets to the console
  val englishStream = branches.head
  englishStream.print(Printed.toSysOut[Array[Byte], Tweet].withLabel("tweets-english"))

  val nonEnglishStream = branches.tail.head
  nonEnglishStream.print(Printed.toSysOut[Array[Byte], Tweet].withLabel("tweets-nonEnglish"))

  // 4. TRANSLATE the nonEnglish stream
  val translatedStream = nonEnglishStream
    .mapValues(tweet => languageClient.translate(tweet, "en"))

  // 5. MERGE the streams back
  val mergedStream = englishStream.merge(translatedStream)

  // 6. ENRICH with sentiment and salience scores
  val enriched = mergedStream
    .flatMapValues(tweet => {
      val results = languageClient.getEntitySentiment(tweet)
      results.filter(es => currencies.contains(es.entity))
    })

  // 7. OUTPUT to registry-aware AVRO topic
  implicit val producedWith: Produced[Array[Byte], EntitySentiment] =
    Produced.`with`(Serdes.byteArraySerde, AvroSerdes.entitySentimentAvro("http://localhost:8081", isKey = false))
  enriched
    .to("crypto-sentiment")

  val topology   = builder.build()
  val properties = new Properties
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-twitter-1")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val streams = new KafkaStreams(topology, properties)
  streams.start()

  sys.ShutdownHookThread {
    streams.close()
  }

}
