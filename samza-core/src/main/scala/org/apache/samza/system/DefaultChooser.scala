package org.apache.samza.system

import org.apache.samza.config.Config
import org.apache.samza.config.DefaultChooserConfig._
import org.apache.samza.config.TaskConfig._

// TODO add logging everywhere
// TODO javadocs for DefaultChooser
class DefaultChooserFactory extends MessageChooserFactory {
  def getChooser(config: Config): MessageChooser = {
    // TODO only fully compose in cases where there are bootstrap and prioritized streams.
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
    val prioritizedStreams = defaultPrioritizedStreams ++ prioritizedBootstrapStreams ++ config.getPriorityStreams
    val prioritizedChoosers = prioritizedStreams
      .values
      .toSet
      .map((_: Int, new BatchingChooser(new RoundRobinChooser, batchSize).asInstanceOf[MessageChooser]))
      .toMap
    val priority = new TieredPriorityChooser(prioritizedStreams, prioritizedChoosers)
    // TODO get last offsets
    val bootstrapping = new BootstrappingChooser(Map(), priority)
    bootstrapping
  }
}