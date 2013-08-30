package org.apache.samza.system

import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConversions._
import org.apache.samza.Partition

class TestSystemConsumers {

  @Test
  def testSystemConumersShouldRegisterStartAndStopChooser {
    val system = "test-system"
    val systemStreamPartition = new SystemStreamPartition(system, "some-stream", new Partition(1))
    var started = 0
    var stopped = 0
    var registered = Set[SystemStreamPartition]()

    val consumer = Map(system -> new SystemConsumer {
      def start {}
      def stop {}
      def register(systemStreamPartition: SystemStreamPartition, lastReadOffset: String) {}
      def poll(systemStreamPartitions: java.util.Map[SystemStreamPartition, java.lang.Integer], timeout: Long) = List()
    })

    val consumers = new SystemConsumers(new MessageChooser {
      def update(envelope: IncomingMessageEnvelope) = Unit
      def choose = null
      def start = started += 1
      def stop = stopped += 1
      def register(systemStreamPartition: SystemStreamPartition) = registered += systemStreamPartition
    }, consumer, null)

    consumers.register(systemStreamPartition, "0")
    consumers.start
    consumers.stop

    assertEquals(1, started)
    assertEquals(1, stopped)
    assertEquals(1, registered.size)
    assertEquals(systemStreamPartition, registered.head)
  }
}