package org.hackweek.xing.StatelessProcessing.language

import org.hackweek.xing.StatelessProcessing.EntitySentiment
import org.hackweek.xing.StatelessProcessing.serialization.Tweet

trait LanguageClient {

  def translate(tweet: Tweet, targetLanguage: String): Tweet

  def getEntitySentiment(tweet: Tweet): List[EntitySentiment]

}
