package org.apache.samza.system

import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.MetricsHelper

class SystemConsumersMetrics(val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val choseNull = newCounter("chose-null")
  val choseObject = newCounter("chose-object")
  val systemPolls = scala.collection.mutable.Map[String, Counter]()
  val systemStreamPartitionFetchesPerPoll = scala.collection.mutable.Map[String, Counter]()
  val systemMessagesPerPoll = scala.collection.mutable.Map[String, Counter]()

  def setUnprocessedMessages(getValue: () => Int) {
    newGauge("unprocessed-messages", getValue)
  }

  def setNeededByChooser(getValue: () => Int) {
    newGauge("ssps-needed-by-chooser", getValue)
  }

  def setTimeout(getValue: () => Long) {
    newGauge("poll-timeout", getValue)
  }

  def setMaxMessagesPerStreamPartition(getValue: () => Int) {
    newGauge("max-buffered-messages-per-stream-partition", getValue)
  }

  def setNoNewMessagesTimeout(getValue: () => Long) {
    newGauge("blocking-poll-timeout", getValue)
  }

  def registerSystem(systemName: String) {
    if (!systemPolls.contains(systemName)) {
      systemPolls += systemName -> newCounter("%s-polls" format systemName)
      systemStreamPartitionFetchesPerPoll += systemName -> newCounter("%s-ssp-fetches-per-poll" format systemName)
      systemMessagesPerPoll += systemName -> newCounter("%s-messages-per-poll" format systemName)
    }
  }
}