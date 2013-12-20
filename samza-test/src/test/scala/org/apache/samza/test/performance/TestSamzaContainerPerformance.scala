package org.apache.samza.test.performance

import org.junit.Test
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.MessageCollector
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.job.local.LocalJobFactory
import org.apache.samza.config.MapConfig
import scala.collection.JavaConversions._
import org.apache.samza.job.ShellCommandBuilder

object TestSamzaContainerPerformance {
  var messagesProcessed = 0
  var startTime = 0L
}

class TestSamzaContainerPerformance {
  val consumerThreadCount = System.getProperty("mock.consumer.thread.count", "12").toInt
  val messagesPerBatch = System.getProperty("mock.messages.per.batch", "5000").toInt
  // was 8
  val streamCount = System.getProperty("mock.input.streams", "1000").toInt
  val partitionsPerStreamCount = System.getProperty("mock.partitions.per.stream", "4").toInt
  val jobConfig = Map(
    "job.factory.class" -> "org.apache.samza.job.local.LocalJobFactory",
    "job.name" -> "test-container-performance",
    "task.class" -> classOf[TestPerformanceTask].getName,
    "task.inputs" -> (0 until streamCount).map(i => "mock.stream" + i).mkString(","),
    "systems.mock.samza.factory" -> "org.apache.samza.system.mock.MockSystemFactory",
    "systems.mock.partitions.per.stream" -> partitionsPerStreamCount.toString,
    "systems.mock.messages.per.batch" -> messagesPerBatch.toString,
    "systems.mock.consumer.thread.count" -> consumerThreadCount.toString)

  @Test
  def testContainerPerformance {
    System.out.println("Testing performance with configuration: %s" format jobConfig)

    val jobFactory = new LocalJobFactory
    val job = jobFactory
      .getJob(new MapConfig(jobConfig))
      .submit

    job.waitForFinish(Int.MaxValue)
  }
}

class TestPerformanceTask extends StreamTask {
  import TestSamzaContainerPerformance._

  def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    if (messagesProcessed == 0) {
      startTime = System.currentTimeMillis
    }

    messagesProcessed += 1

    if (messagesProcessed % 10000 == 0) {
      val seconds = (System.currentTimeMillis - startTime) / 1000
      System.err.println("Processed %s messages in %s seconds." format (messagesProcessed, seconds))
    }

    if (messagesProcessed >= 1000000) {
      coordinator.shutdown
    }
  }
}