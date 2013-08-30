package org.apache.samza.metrics

import org.junit.Test
import org.junit.Assert._
import org.apache.samza.container.SamzaContainerMetrics

class TestMetricsHelper {
  @Test
  def testMetricsHelperGroupShouldBePAckageName {
    System.err.println(classOf[SamzaContainerMetrics].getName)
    assertEquals(classOf[SamzaContainerMetrics].getName, new SamzaContainerMetrics().GROUP)
  }
}