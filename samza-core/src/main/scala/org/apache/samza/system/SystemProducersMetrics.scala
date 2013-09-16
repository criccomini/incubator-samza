package org.apache.samza.system

import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.metrics.Counter

class SystemProducersMetrics(val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val flushes = newCounter("flushes")
  val sends = newCounter("sends")
  val sourceFlushes = scala.collection.mutable.Map[String, Counter]()
  val sourceSends = scala.collection.mutable.Map[String, Counter]()

  def registerSource(source: String) {
    sourceFlushes += source -> newCounter("%s-flushes" format source)
    sourceSends += source -> newCounter("%s-sends" format source)
  }
}
