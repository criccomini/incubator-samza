package org.apache.samza.metrics

import org.junit.Test
import org.junit.Assert._
import org.apache.samza.container.SamzaContainerMetrics

class TestMetricsHelper {
  @Test
  def testMetricsHelperGroupShouldBePAckageName {
    assertEquals(classOf[SamzaContainerMetrics].getName, new SamzaContainerMetrics().GROUP)
  }
}
