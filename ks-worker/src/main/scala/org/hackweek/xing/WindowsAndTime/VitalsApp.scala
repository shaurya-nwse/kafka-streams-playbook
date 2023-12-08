package org.hackweek.xing.WindowsAndTime

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{JoinWindows, Printed, Suppressed, TimeWindows, Windowed}
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.scala.{ByteArrayWindowStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable, Materialized}
import org.hackweek.xing.WindowsAndTime.extractor.VitalTimestampExtractor
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.HostInfo
import org.hackweek.xing.WindowsAndTime.model.{BodyTemp, CombinedVitals, Pulse}
import org.hackweek.xing.WindowsAndTime.serde.PatientEventsSerdes._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import java.time.Duration
import java.util.Properties

object VitalsApp extends App {

  private val log = LoggerFactory.getLogger(VitalsApp.getClass)

  val builder                             = new StreamsBuilder
  implicit val stringSerde: Serde[String] = Serdes.stringSerde
  implicit val longSerde: Serde[Long]     = Serdes.longSerde

  // CONFIG to increase compaction freq and reduce size of segments
  val changelogTopicConfig = Map("segment.bytes" -> "536870912", "min.cleanable.dirty.ratio" -> "0.3").asJava

  // 1. Pulse Events consumer options; the serde comes from implicits, so we only need to add the TS extractor
  implicit val pulseConsumed: Consumed[String, Pulse] = Consumed
    .`with`(Serdes.stringSerde, implicitly[Serde[Pulse]])
    .withTimestampExtractor(new VitalTimestampExtractor)

  val pulseStream: KStream[String, Pulse] = builder.stream("pulse-events")

  // 2. BodyTemp Events consumer options
  implicit val bodyTempCommand: Consumed[String, BodyTemp] = Consumed
    .`with`(Serdes.stringSerde, implicitly[Serde[BodyTemp]])
    .withTimestampExtractor(new VitalTimestampExtractor)

  val bodyEventStream: KStream[String, BodyTemp] = builder.stream("body-temp-events")

  // 3. Windowed Aggregation
  val tumblingWindow = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(5))

  implicit val materialized: Materialized[String, Long, ByteArrayWindowStore] = Materialized
    .as[String, Long, ByteArrayWindowStore]("pulse-counts")
    .withRetention(Duration.ofHours(6))
    .withLoggingEnabled(changelogTopicConfig)

  val pulseCounts = pulseStream.groupByKey
    .windowedBy(tumblingWindow)
    .count()
    .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull()))

  pulseCounts.toStream.print(Printed.toSysOut[Windowed[String], Long].withLabel("pulse-counts"))

  // 4. Filter and Rekey
  val highPulse = pulseCounts.toStream
    .peek((k, v) => {
      val id    = new String(k.key())
      val start = k.window().start()
      val end   = k.window().end()
      log.info(s"Patient $id had a heart rate of $v between $start and $end")
    })
    .filter((k, v) => v >= 100)
    .selectKey((wk, v) => wk.key())

  // debug
  highPulse.print(Printed.toSysOut[String, Long].withLabel("high-pulse"))

  val highTemp = bodyEventStream.filter((k, v) => v.temp > 100.4)

  // debug
  highTemp.print(Printed.toSysOut[String, BodyTemp].withLabel("high-temp"))

  // 5. Windowed Join Streams
  val streamJoinWindow = JoinWindows
    .of(Duration.ofSeconds(60))
    .grace(Duration.ofSeconds(10))

  val vitalsJoined = highPulse
    .join(highTemp)((pulseRate, bodyTemp) => CombinedVitals(pulseRate.intValue(), bodyTemp), streamJoinWindow)

  // debug
  vitalsJoined.print(Printed.toSysOut[String, CombinedVitals].withLabel("vitals-joined"))

  // 6. Send to alerts topic
  vitalsJoined.to("alerts")

  // App
  val topology = builder.build()
  val props    = new Properties
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-consumer-1")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7001")
  props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")

  // to change timestamp extractor; we override this with VitalTimestampExtractor
  props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
  val streams = new KafkaStreams(topology, props)

  // IMPORTANT: Setting state listener
  streams.setStateListener((oldState, newState) => {
    // Send metrics to Prometheus here
    log.info(s"Application transitioned from $oldState to $newState")
    if (newState.equals(State.REBALANCING)) log.warn(s"Application is re-balancing")
  })

  // IMPORTANT: State restore listener; replays changelog topic back to the state store on startup
  streams.setGlobalStateRestoreListener(new CustomRestoreListener)

  sys.ShutdownHookThread {
    streams.close()
  }

  streams.cleanUp()
  streams.start()

  // REST service
  val hostInfo = new HostInfo("localhost", 7001)
  val service  = new RestService(hostInfo, streams)
  service.start()

}
