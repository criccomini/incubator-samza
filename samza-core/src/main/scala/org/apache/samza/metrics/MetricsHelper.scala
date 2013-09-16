package org.apache.samza.metrics

import org.apache.samza.container.SamzaContainerMetrics

/**
 * MetricsHelper is a little helper class to make it easy to register and
 * manage counters and gauges.
 */
trait MetricsHelper {
  val group = this.getClass.getName
  val registry: MetricsRegistry

  def newCounter(name: String) = {
    registry.newCounter(group, getPrefix + name)
  }

  def newGauge[T](name: String, value: T) = {
    registry.newGauge(group, new Gauge(getPrefix + name, value))
  }

  def newGauge[T](name: String, getValue: () => T) = {
    registry.newGauge(group, new Gauge(getPrefix + name, getValue()) {
      override def getValue() = getValue()
    })
  }

  /**
   * Returns a prefix for metric names.
   */
  def getPrefix = ""
}