package org.hackweek.xing.StatefulProcessing.aggregators

import org.hackweek.xing.StatefulProcessing.model.Enriched

import scala.collection.mutable
import scala.collection.JavaConverters._

class HighScores extends Serializable {

  private final val highScores = new mutable.TreeSet[Enriched]()

  def add(enriched: Enriched): HighScores = {
    highScores.add(enriched);

    if (highScores.size > 3) highScores.remove(highScores.last)
    this
  }

  def toList: java.util.List[Enriched] = {
    highScores.toList.asJava
  }

}
