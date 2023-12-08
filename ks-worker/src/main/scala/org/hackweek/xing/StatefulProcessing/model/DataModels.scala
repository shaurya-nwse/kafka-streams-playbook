package org.hackweek.xing.StatefulProcessing.model

import play.api.libs.json.Json

// TODO: single inherit implicits
sealed trait DataModels
case class ScoreEvent(playerId: Long, productId: Long, score: Double) extends DataModels
object ScoreEvent {
  implicit val writer = Json.writes[ScoreEvent]
  implicit val reads  = Json.format[ScoreEvent]
}

case class Player(id: Long, name: String) extends DataModels
object Player {
  implicit val writer = Json.writes[Player]
  implicit val reads  = Json.format[Player]
}

case class Product(id: Long, name: String) extends DataModels
object Product {
  implicit val writer = Json.writes[Product]
  implicit val reads  = Json.format[Product]
}

sealed trait JoinDataModels
case class ScoreWithPlayer(scoreEvent: ScoreEvent, player: Player) extends JoinDataModels
object ScoreWithPlayer {
  implicit val writer = Json.writes[ScoreWithPlayer]
  implicit val reads  = Json.format[ScoreWithPlayer]
}

case class Enriched(playerId: Long, productId: Long, playerName: String, gameName: String, score: Double) extends Comparable[Enriched] with JoinDataModels {
  override def compareTo(o: Enriched): Int = java.lang.Double.compare(o.score, score)
}
object Enriched {
  implicit val writer = Json.writes[Enriched]
  implicit val reads  = Json.format[Enriched]

  def apply(scoreWithPlayer: ScoreWithPlayer, product: Product): Enriched = {
    Enriched(scoreWithPlayer.player.id, product.id, scoreWithPlayer.player.name, product.name, scoreWithPlayer.scoreEvent.score)
  }
}
