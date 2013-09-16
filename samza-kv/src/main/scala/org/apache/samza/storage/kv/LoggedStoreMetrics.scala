package org.apache.samza.storage.kv

import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.MetricsHelper

class LoggedStoreMetrics(
  val storeName: String = "unknown",
  val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  val gets = newCounter("gets")
  val ranges = newCounter("ranges")
  val alls = newCounter("alls")
  val puts = newCounter("puts")
  val deletes = newCounter("deletes")
  val flushes = newCounter("flushes")

  override def getPrefix = storeName + "-"
}