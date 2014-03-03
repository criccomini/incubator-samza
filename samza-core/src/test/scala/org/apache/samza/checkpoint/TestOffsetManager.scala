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

package org.apache.samza.checkpoint

import scala.collection.JavaConversions._
import org.apache.samza.Partition
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.OffsetType
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.SystemStreamPartition
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.apache.samza.SamzaException
import org.apache.samza.util.TestUtil._

class TestOffsetManager {
  @Test
  def testShouldUseDefaults {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val defaultOffsets = Map(systemStream -> OffsetType.OLDEST)
    val offsetManager = new OffsetManager(
      systemStreamMetadata,
      defaultOffsets)
    offsetManager.register(systemStreamPartition)
    offsetManager.start
    assertTrue(offsetManager(systemStreamPartition).isDefined)
    assertEquals("0", offsetManager(systemStreamPartition).get)
  }

  @Test
  def testShouldLoadFromAndSaveWithCheckpointManager {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val checkpointManager = getCheckpointManager(systemStreamPartition)
    val offsetManager = new OffsetManager(checkpointManager = checkpointManager)
    offsetManager.register(systemStreamPartition)
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertEquals(partition, checkpointManager.registered.head)
    assertEquals(checkpointManager.checkpoints.head._2, checkpointManager.readLastCheckpoint(partition))
    // Should get offset 45 back from the checkpoint manager.
    assertEquals("45", offsetManager(systemStreamPartition).get)
    offsetManager.update(systemStreamPartition, "46")
    assertEquals("46", offsetManager(systemStreamPartition).get)
    offsetManager.checkpoint(partition)
    val expectedCheckpoint = new Checkpoint(Map(systemStream -> "46"))
    assertEquals(expectedCheckpoint, checkpointManager.readLastCheckpoint(partition))
  }

  @Test
  def testShouldResetStreams {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val defaultOffsets = Map(systemStream -> OffsetType.OLDEST)
    val checkpoint = new Checkpoint(Map(systemStream -> "45"))
    val checkpointManager = getCheckpointManager(systemStreamPartition)
    val offsetManager = new OffsetManager(
      systemStreamMetadata,
      defaultOffsets,
      resetOffsets = Set(systemStream),
      checkpointManager)
    offsetManager.register(systemStreamPartition)
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertEquals(partition, checkpointManager.registered.head)
    assertEquals(checkpoint, checkpointManager.readLastCheckpoint(partition))
    // Should be zero even though the checkpoint has an offset of 45, since we're forcing a reset.
    assertEquals("0", offsetManager(systemStreamPartition).get)
  }

  @Test
  def testShouldFailWhenMissingMetadata {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val offsetManager = new OffsetManager
    offsetManager.register(systemStreamPartition)

    expect(classOf[SamzaException], Some("No metadata available for SystemStream [system=test-system, stream=test-stream]. Can't continue without this information.")) {
      offsetManager.start
    }
  }

  @Test
  def testShouldFailWhenMissingDefault {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val offsetManager = new OffsetManager(systemStreamMetadata)
    offsetManager.register(systemStreamPartition)

    expect(classOf[SamzaException], Some("No default offeset defined for SystemStream [system=test-system, stream=test-stream]. Unable to load a default.")) {
      offsetManager.start
    }
  }

  private def getCheckpointManager(systemStreamPartition: SystemStreamPartition) = {
    val checkpoint = new Checkpoint(Map(systemStreamPartition.getSystemStream -> "45"))

    new CheckpointManager {
      var isStarted = false
      var isStopped = false
      var registered = Set[Partition]()
      var checkpoints = Map(systemStreamPartition.getPartition -> checkpoint)
      def start { isStarted = true }
      def register(partition: Partition) { registered += partition }
      def writeCheckpoint(partition: Partition, checkpoint: Checkpoint) { checkpoints += partition -> checkpoint }
      def readLastCheckpoint(partition: Partition) = checkpoints(partition)
      def stop { isStopped = true }
    }
  }
}