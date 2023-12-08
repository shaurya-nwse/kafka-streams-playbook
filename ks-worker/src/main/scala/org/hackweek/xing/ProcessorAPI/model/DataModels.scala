package org.hackweek.xing.ProcessorAPI.model

import play.api.libs.json.{Json, Reads, Writes}

// Power
object Power extends Enumeration {
  val ON, OFF             = Value
  implicit val typeReads  = Reads.enumNameReads(Power)
  implicit val typeWrites = Writes.enumNameWrites
}

// StateType
object StateType extends Enumeration {
  val DESIRED, REPORTED   = Value
  implicit val typeReads  = Reads.enumNameReads(StateType)
  implicit val typeWrites = Writes.enumNameWrites
}

case class TurbineState(timestamp: String, windSpeedMph: Double, power: Power.Value, stateType: StateType.Value)
object TurbineState {
  implicit val writer = Json.writes[TurbineState]
  implicit val reader = Json.format[TurbineState]
}

// For joined
case class DigitalTwin(var desired: Option[TurbineState], var reported: Option[TurbineState])
object DigitalTwin {
  implicit val writer = Json.writes[DigitalTwin]
  implicit val reader = Json.format[DigitalTwin]
}

object DataModels extends App {
  val obj1 = DigitalTwin(Some(TurbineState("123", 4.6, Power.ON, StateType.DESIRED)), Some(TurbineState("123", 4.6, Power.OFF, StateType.REPORTED)))
  println(Json.toJson(obj1))

  val obj2 = DigitalTwin(None, None)
  println(Json.toJson(obj2))

  val obj3 = DigitalTwin(None, None)
  obj3.desired = Some(TurbineState("123", 4.6, Power.ON, StateType.DESIRED))
  println(Json.toJson(obj3))
}
