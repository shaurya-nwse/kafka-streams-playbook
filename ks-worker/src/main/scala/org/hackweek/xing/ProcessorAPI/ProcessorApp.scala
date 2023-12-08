package org.hackweek.xing.ProcessorAPI

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.hackweek.xing.ProcessorAPI.model.{DigitalTwin, TurbineState}
import org.hackweek.xing.ProcessorAPI.serde.DigitalTwinSerde._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.{HostInfo, KeyValueStore, StoreBuilder, Stores}
import org.hackweek.xing.ProcessorAPI.processors.{DigitalTwinProcessor, HighWindsFlatmapProcessor}
import org.slf4j.LoggerFactory

import java.util.Properties

object ProcessorApp extends App {

  private val log    = LoggerFactory.getLogger(ProcessorApp.getClass)
  private val config = ConfigFactory.load().getConfig("streams")

  val props = new Properties
  config.entrySet().forEach(e => props.setProperty(e.getKey, config.getString(e.getKey)))

  val streams = new KafkaStreams(getTopology, props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close()
  }

  val hostInfo = new HostInfo("localhost", 7001)
  val service  = new RestService(hostInfo, streams)
  service.start()

  def getTopology: Topology = {
    val builder = new Topology

    // only needs the deserializer
    // Desired state events
    builder.addSource("Desired State Events", Serdes.stringSerde.deserializer(), implicitly[Serde[TurbineState]].deserializer(), "desired-state-events")
    // Reported state events
    builder.addSource("Reported State Events", Serdes.stringSerde.deserializer(), implicitly[Serde[TurbineState]].deserializer(), "reported-state-events")
    // Stateless stream processor
    builder.addProcessor(
      "High Winds Flatmap Processor",
      new ProcessorSupplier[String, TurbineState, String, TurbineState] {
        override def get(): Processor[String, TurbineState, String, TurbineState] = new HighWindsFlatmapProcessor
      },
      "Reported State Events"
    )

    // Stateful processor combining desired and reported events; ADD BEFORE ADDING STORE
    builder.addProcessor(
      "Digital Twin Processor",
      new ProcessorSupplier[String, TurbineState, String, DigitalTwin] {
        override def get(): Processor[String, TurbineState, String, DigitalTwin] = new DigitalTwinProcessor
      },
      "High Winds Flatmap Processor",
      "Desired State Events"
    )

    // Add state store and connect it to Digital twin stateful processor
    val storeBuilder: StoreBuilder[KeyValueStore[String, DigitalTwin]] = Stores
      .keyValueStoreBuilder(
        Stores.persistentKeyValueStore("digital-twin-store"),
        Serdes.stringSerde,
        implicitly[Serde[DigitalTwin]]
      )
    builder.addStateStore(storeBuilder, "Digital Twin Processor")

    // Sink processor; write to an output topic
    builder.addSink(
      "Digital Twin Sink",
      "digital-twins",
      Serdes.stringSerde.serializer(),
      implicitly[Serde[DigitalTwin]].serializer(),
      "Digital Twin Processor"
    )

    // return the builder
    builder
  }

}
