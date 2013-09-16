package org.apache.samza.system.kafka

import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.ReadableMetricsRegistry
import org.apache.samza.system.SystemStream
import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.Partition
import org.apache.samza.metrics.Gauge
import org.apache.samza.metrics.MetricsRegistry

class KafkaSystemProducerMetrics(val systemName: String, val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val reconnects = newCounter("%s-producer-reconnects" format systemName)
}
