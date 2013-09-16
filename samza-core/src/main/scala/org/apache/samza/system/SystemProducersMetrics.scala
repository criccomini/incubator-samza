package org.apache.samza.system

import org.apache.samza.metrics.ReadableMetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.metrics.Counter

class SystemProducersMetrics(
  val containerName: String = "unnamed-container",
  implicit val registry: ReadableMetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  val SOURCE = containerName
  val flushes = newCounter("flushes")
  val sends = newCounter("sends")
  val sourceFlushes = scala.collection.mutable.Map[String, Counter]()
  val sourceSends = scala.collection.mutable.Map[String, Counter]()

  def registerSource(source: String) {
    sourceFlushes += source -> newCounter("flushes-%s" format source)
    sourceSends += source -> newCounter("sends-%s" format source)
  }
}
