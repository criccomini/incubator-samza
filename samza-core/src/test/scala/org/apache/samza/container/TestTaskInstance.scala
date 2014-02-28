/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.container

import scala.collection.JavaConversions._
import org.apache.samza.Partition
import org.apache.samza.config.MapConfig
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemConsumers
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.OffsetType
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.checkpoint.OffsetManager

class TestTaskInstance {
  @Test
  def testOffsetsAreUpdatedOnProcess {
    val task = new StreamTask {
      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
      }
    }
    val config = new MapConfig
    val partition = new Partition(0)
    val containerName = "test-container"
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val systemStream = new SystemStream("test-system", "test-stream")
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    // Pretend our last checkpointed (next) offset was 2.
    val testSystemStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val offsetManager = new OffsetManager(
      streamMetadata = Map(systemStream -> testSystemStreamMetadata),
      defaultOffsets = Map(systemStream -> OffsetType.UPCOMING))
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      partition,
      config,
      new TaskInstanceMetrics,
      consumerMultiplexer,
      producerMultiplexer,
      offsetManager)
    // Pretend we got a message with offset 2 and next offset 3.
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "2", "3", null, null), new ReadableCoordinator)
    // Check to see if the offset manager has been properly updated with offset 3.
    val newNextOffset = offsetManager(systemStreamPartition)
    assertTrue(newNextOffset.isDefined)
    assertEquals("3", newNextOffset.get)
  }
}