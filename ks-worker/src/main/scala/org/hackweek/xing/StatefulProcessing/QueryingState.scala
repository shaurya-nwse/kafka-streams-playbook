//package org.hackweek.xing.StatefulProcessing
//
//import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
//import org.apache.kafka.streams.state.{KeyValueIterator, QueryableStoreTypes, ReadOnlyKeyValueStore}
//import org.hackweek.xing.StatefulProcessing.aggregators.HighScores
//
//import scala.collection.JavaConverters._
//
//object QueryingState {
//
//  def queryState(streams: KafkaStreams) = {
//    val stateStore: ReadOnlyKeyValueStore[String, HighScores] = streams.store(
//      StoreQueryParameters.fromNameAndType(
//        "leader-boards",
//        QueryableStoreTypes.keyValueStore()
//      )
//    )
//
//    // Print all
//    val leaderboard: KeyValueIterator[String, HighScores] = stateStore.all()
//    leaderboard.asScala.toList.foreach(println)
//
//  }
//}
