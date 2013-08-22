package org.apache.samza.system

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.Partition
import org.apache.samza.config.MapConfig
import org.apache.samza.config.StreamChooserConfig
import scala.collection.JavaConversions._

class TestStreamChooser {
  @Test
  def testStreamChooser {
    val chooserFactory = new StreamChooserFactory
    val config = new MapConfig(Map(StreamChooserConfig.PRIORITIZED_STREAM_ORDER -> "kafka.stream1,kafka.stream2"))
    val chooser = chooserFactory.getChooser(config)
    val systemStream1 = new SystemStream("kafka", "stream1")
    val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(0)), null, null, 1);
    val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(1)), null, null, 1);
    val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream2", new Partition(0)), null, null, 3);

    assertEquals(null, chooser.choose)

    // Test one message.
    chooser.update(envelope2)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)

    // Verify in order ordering.
    chooser.update(envelope1)
    chooser.update(envelope3)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)

    // Verify out of order ordering.
    chooser.update(envelope3)
    chooser.update(envelope2)
    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)

    // Verify same stream different partition.
    chooser.update(envelope3)
    chooser.update(envelope2)
    chooser.update(envelope1)

    // Ordering of equal items with equal priorit is undefined, so just verify system/stream.
    assertTrue(systemStream1.equals(chooser.choose.getSystemStreamPartition.getSystemStream))
    assertTrue(systemStream1.equals(chooser.choose.getSystemStreamPartition.getSystemStream))
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)
  }
}