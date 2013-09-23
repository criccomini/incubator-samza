package org.apache.samza.system

import org.apache.samza.config.Config
import org.apache.samza.config.DefaultChooserConfig._
import org.apache.samza.config.TaskConfig._

// TODO javadocs for DefaultChooser
class DefaultChooserFactory extends MessageChooserFactory {
  def getChooser(config: Config): MessageChooser = {
    val batchSize = config
      .getChooserBatchSize
      .getOrElse("100")
      .toInt
    val prioritizedBootstrapStreams = config
      .getBootstrapStreams
      .map((_, Int.MaxValue))
      .toMap
    val defaultPrioritizedStreams = config
      .getInputStreams
      .map((_, 0))
      .toMap
    val prioritizedStreams = defaultPrioritizedStreams ++ config.getPriorityStreams ++ prioritizedBootstrapStreams
    val base = new RoundRobinChooser
    val batching = new BatchingChooser(base, batchSize)
    val prioritizedChoosers = prioritizedStreams
      .values
      .toSet
      .map((_: Int, new BatchingChooser(base, batchSize).asInstanceOf[MessageChooser]))
      .toMap
    val priority = new TieredPriorityChooser(prioritizedStreams, prioritizedChoosers)
    val bootstrapping = new BootstrappingChooser(Map(), priority)
    bootstrapping
  }
}