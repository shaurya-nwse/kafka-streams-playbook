package org.hackweek.xing.StatelessProcessing.language

import org.hackweek.xing.StatelessProcessing.EntitySentiment
import org.hackweek.xing.StatelessProcessing.serialization.Tweet

import java.util.concurrent.ThreadLocalRandom

class DummyClient extends LanguageClient {

  import DummyClient._

  override def translate(tweet: Tweet, targetLanguage: String): Tweet = {
    tweet.copy(text = s"Translated: ${tweet.text}")
  }

  override def getEntitySentiment(tweet: Tweet): List[EntitySentiment] = {
    tweet.text
      .toLowerCase()
      .replace("#", "")
      .split(" ")
      .foldLeft(List.empty[EntitySentiment]) { (lst, word) =>
        EntitySentiment(
          tweet.createdAt,
          tweet.id,
          word,
          tweet.text,
          randomDouble(),
          randomDouble(),
          randomDouble()
        ) :: lst
      }
  }

}

object DummyClient {

  def randomDouble(): Double = {
    ThreadLocalRandom.current().nextDouble(0, 1)
  }

}
