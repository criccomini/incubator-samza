package org.apache.samza.storage.kv

import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.MetricsHelper

class LevelDbKeyValueStoreMetrics(
  val storeName: String = "unknown",
  val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  val gets = newCounter("gets")
  val ranges = newCounter("ranges")
  val alls = newCounter("alls")
  val puts = newCounter("puts")
  val deletes = newCounter("deletes")
  val flushes = newCounter("flushes")
  val bytesWritten = newCounter("bytes-written")
  val bytesRead = newCounter("bytes-read")

  override def getPrefix = storeName + "-"
}