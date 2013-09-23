package org.apache.samza.system

import org.apache.samza.config.Config

class DefaultChooserFactory extends MessageChooserFactory {
  def getChooser(config: Config): MessageChooser = {
    val base = new RoundRobinChooser
    val batching = new BatchingChooser(base)
    val priority = new TieredPriorityChooser(Map(), Map())
    val bootstrapping = new BootstrappingChooser(Map(), priority)
    bootstrapping
  }
}