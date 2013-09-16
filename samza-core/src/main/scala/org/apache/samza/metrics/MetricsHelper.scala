package org.apache.samza.metrics

import org.apache.samza.container.SamzaContainerMetrics

/**
 * MetricsHelper is a little helper class to make it easy to register and
 * manage counters and gauges.
 */
trait MetricsHelper {
  val group = this.getClass.getName
  val registry: MetricsRegistry

  val newCounter = registry.newCounter(group, _: String)

  def newGauge[T](name: String, value: T) = {
    registry.newGauge(group, new Gauge(name, value))
  }

  def newGauge[T](name: String, getValue: () => T) = {
    registry.newGauge(group, new Gauge(name, getValue()) {
      override def getValue() = getValue()
    })
  }
}