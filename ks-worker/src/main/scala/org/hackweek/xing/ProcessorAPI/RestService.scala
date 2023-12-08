package org.hackweek.xing.ProcessorAPI

import io.javalin.Javalin
import io.javalin.http.Context
import okhttp3.{OkHttpClient, Request}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{HostInfo, QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.hackweek.xing.ProcessorAPI.model.DigitalTwin
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class RestService(hostInfo: HostInfo, streams: KafkaStreams) {

  import RestService._

  def start(): Unit = {
    val app = Javalin.create().start(hostInfo.port())
    app.get("/device/{id}", ctx => this.getDevice(ctx))
  }

  def getStore: ReadOnlyKeyValueStore[String, DigitalTwin] = streams.store(
    StoreQueryParameters.fromNameAndType(STORE, QueryableStoreTypes.keyValueStore[String, DigitalTwin]())
  )

  def getDevice(context: Context): Unit = {
    val deviceId = context.pathParam("id")

    // which host has the key
    val metadata   = streams.queryMetadataForKey(STORE, deviceId, Serdes.stringSerde.serializer())
    val activeHost = metadata.activeHost()

    // If local instance has the key (localhost)
    if (hostInfo.equals(activeHost)) {
      log.info("Querying local state for key")
      val latestState = getStore.get(deviceId)

      latestState match {
        case null => context.status(404)
        case _    => context.json(latestState)
      }
    }

    // If remote instance has the key
    val url    = s"http://${activeHost.host()}:${activeHost.port()}/devices/$deviceId"
    val client = new OkHttpClient()
    val request = new Request.Builder()
      .url(url)
      .build()

    val response = Try(client.newCall(request).execute())
    response match {
      case Success(result) =>
        log.info("Querying remote store for key")
        context.result(result.body().string())
      case Failure(exception) =>
        log.error(s"Error querying remote store, ${exception.printStackTrace()}")
        context.status(500)
    }
  }

}

object RestService {
  private val STORE = "digital-twin-store"
  private val log   = LoggerFactory.getLogger(RestService.getClass)
}
