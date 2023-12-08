package org.hackweek.xing.WindowsAndTime

import io.javalin.Javalin
import io.javalin.http.Context
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{HostInfo, QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.hackweek.xing.WindowsAndTime.model.CombinedVitals
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.collection.JavaConverters._

class RestService(hostInfo: HostInfo, streams: KafkaStreams) {

  private val log = LoggerFactory.getLogger(getClass)

  def getBpmStore(): ReadOnlyWindowStore[String, Long] = {
    streams.store(
      StoreQueryParameters.fromNameAndType("pulse-counts", QueryableStoreTypes.windowStore[String, Long]())
    )
  }

  def getAlertsStore(): ReadOnlyKeyValueStore[String, CombinedVitals] = streams.store(
    StoreQueryParameters.fromNameAndType("alerts", QueryableStoreTypes.keyValueStore[String, CombinedVitals]())
  )

  def start(): Unit = {
    val app = Javalin.create().start(hostInfo.port())

    // routes
    app.get("/bpm/all", ctx => this.getAll(ctx))
    app.get("/bpm/range/{from}/{to}", ctx => this.getAllInRange(ctx))
    app.get("/bpm/range/{key}/{from}/{to}", ctx => this.getRange(ctx))

  }

  // All keys
  def getAll(context: Context): Unit = {
    val range = getBpmStore().all().asScala
    context.json(
      range
        .foldLeft(Map.empty[String, Long])((m, v) => m + (v.key.toString -> v.value))
        .asJava
    )
  }

  // All Keys in a window
  def getAllInRange(context: Context): Unit = {
    val from = context.pathParam("from")
    val to   = context.pathParam("to")

    val fromTime = Instant.ofEpochMilli(java.lang.Long.valueOf(from))
    val toTime   = Instant.ofEpochMilli(java.lang.Long.valueOf(to))

    val range = getBpmStore().fetchAll(fromTime, toTime).asScala

    context.json(
      range
        .foldLeft(List.empty[Map[String, Any]])((l, o) => {
          Map("key" -> o.key, "start" -> o.key.window().start(), "end" -> o.key.window().end(), "count" -> o.value) :: l
        })
        .asJava
    )
  }

  // Specific key in a window
  def getRange(context: Context): Unit = {
    val key  = context.pathParam("key")
    val from = context.pathParam("from")
    val to   = context.pathParam("to")

    val fromTime = Instant.ofEpochMilli(java.lang.Long.valueOf(from))
    val toTime   = Instant.ofEpochMilli(java.lang.Long.valueOf(to))

    val range = getBpmStore().fetch(key, fromTime, toTime).asScala

    context.json(
      range
        .foldLeft(List.empty[Map[String, Any]])((l, o) => {
          Map("timestamp" -> Instant.ofEpochMilli(o.key).toString, "count" -> o.value) :: l
        })
        .asJava
    )
  }

}
