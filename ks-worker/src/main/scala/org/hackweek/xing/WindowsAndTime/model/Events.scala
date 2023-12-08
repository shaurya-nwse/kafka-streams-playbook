package org.hackweek.xing.WindowsAndTime.model

import play.api.libs.json.{Json, OFormat, OWrites}

sealed trait PatientEvents
case class Pulse(timestamp: String) extends Vital with PatientEvents {
  override def getTimestamp: String = this.timestamp
}
object Pulse {
  implicit val writer: OWrites[Pulse] = Json.writes[Pulse]
  implicit val reader: OFormat[Pulse] = Json.format[Pulse]
}

case class BodyTemp(timestamp: String, temp: Double, unit: String) extends Vital with PatientEvents {
  override def getTimestamp: String = this.timestamp
}
object BodyTemp {
  implicit val writer: OWrites[BodyTemp] = Json.writes[BodyTemp]
  implicit val reader: OFormat[BodyTemp] = Json.format[BodyTemp]
}

sealed trait JoinedEvents
case class CombinedVitals(heartRate: Int, bodyTemp: BodyTemp) extends JoinedEvents
object CombinedVitals {
  implicit val writer: OWrites[CombinedVitals] = Json.writes[CombinedVitals]
  implicit val reader: OFormat[CombinedVitals] = Json.format[CombinedVitals]
}
