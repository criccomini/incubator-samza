package org.apache.samza.system

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.Partition

class TestRoundRobinChooser {
  @Test
  def testRoundRobinChooser {
    val chooser = new RoundRobinChooser
    val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), null, null, 1);
    val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(1)), null, null, 2);
    val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(0)), null, null, 3);

    assertEquals(null, chooser.choose)

    // Test one message.
    chooser.update(envelope1)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)

    // Verify simple ordering.
    chooser.update(envelope1)
    chooser.update(envelope2)
    chooser.update(envelope3)

    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)

    // Verify mixed ordering.
    chooser.update(envelope2)
    chooser.update(envelope1)

    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope1, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope2)

    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)

    // Verify simple ordering with different starting envelope.
    chooser.update(envelope2)
    chooser.update(envelope1)
    chooser.update(envelope3)

    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)
  }
}