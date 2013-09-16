package org.apache.samza.metrics

import org.apache.samza.container.SamzaContainerMetrics

object MetricsHelper {
  def main(args: Array[String]) {
    println(new SamzaContainerMetrics().GROUP)
  }
}

/**
 * MetricsHelper is a little helper class to make it easy to register and
 * manage counters and gauges.
 */
trait MetricsHelper {
  val GROUP = this.getClass.getName
  val SOURCE: String
  val registry: MetricsRegistry

  val newCounter = registry.newCounter(GROUP, _: String)

  def newGauge[T](name: String, getValue: () => T) = {
    registry.newGauge(GROUP, new Gauge(name, getValue()) {
      override def getValue() = getValue()
    })
  }
}