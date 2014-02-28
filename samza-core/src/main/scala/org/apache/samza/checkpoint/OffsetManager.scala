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

import org.apache.samza.checkpoint.CheckpointManager
import org.apache.samza.system.SystemStream
import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.OffsetType
import org.apache.samza.SamzaException
import scala.collection.JavaConversions._
import grizzled.slf4j.Logging

class OffsetManager(
  streamMetadata: Map[SystemStream, SystemStreamMetadata] = Map(),
  defaultOffsets: Map[SystemStream, OffsetType] = Map(),
  resetOffsets: Set[SystemStream] = Set(),
  checkpointManager: CheckpointManager = null) extends Logging {

  var offsets = Map[SystemStreamPartition, String]()
  var partitions = Set[Partition]()

  def apply(systemStreamPartition: SystemStreamPartition) = {
    offsets.get(systemStreamPartition)
  }

  def register(partition: Partition) {
    if (checkpointManager != null) {
      checkpointManager.register(partition)
    }

    partitions += partition
  }

  def start {
    loadOffsetsFromCheckpointManager
    stripResetStreams
    loadDefaults

    info("Successfully loaded offsets: %s" format offsets)
  }

  def update(systemStreamPartition: SystemStreamPartition, nextOffset: String) {
    offsets += systemStreamPartition -> nextOffset
  }

  def checkpoint(partition: Partition) {
    if (checkpointManager != null) {
      debug("Checkpointing offsets for partition %s." format partition)

      val partitionOffsets = offsets
        .filterKeys(_.getPartition.equals(partition))
        .map { case (systemStreamPartition, offset) => (systemStreamPartition.getSystemStream, offset) }
        .toMap

      checkpointManager.writeCheckpoint(partition, new Checkpoint(partitionOffsets))
    } else {
      debug("Skipping checkpointing for partition %s because no checkpoint manager is defined." format partition)
    }
  }

  def stop {
    if (checkpointManager != null) {
      debug("Shutting down checkpoint manager.")

      checkpointManager.stop
    } else {
      debug("Skipping checkpoint manager shutdown because no checkpoint manager is defined.")
    }
  }

  private def loadOffsetsFromCheckpointManager {
    if (checkpointManager != null) {
      debug("Loading offsets from checkpoint manager.")

      checkpointManager.start

      offsets ++= partitions.flatMap(restoreOffsetsFromCheckpoint(_))
    } else {
      debug("Skipping offset load from checkpoint manager because no manager was defined.")
    }
  }

  private def restoreOffsetsFromCheckpoint(partition: Partition) = {
    debug("Loading checkpoints for partition: %s." format partition)

    checkpointManager
      .readLastCheckpoint(partition)
      .getOffsets
      .map { case (systemStream, offset) => (new SystemStreamPartition(systemStream, partition), offset) }
      .toMap
  }

  private def stripResetStreams {
    offsets = offsets.filter {
      case (systemStreamPartition, offset) =>
        if (resetOffsets.contains(systemStreamPartition.getSystemStream)) {
          info("Got offset %s for %s, but ignoring, since stream was configured to reset offsets." format (offset, systemStreamPartition))

          false
        } else {
          true
        }
    }
  }

  private def loadDefaults {
    partitions.foreach(partition => {
      streamMetadata.foreach {
        case (systemStream, systemStreamMetadata) =>
          val systemStreamPartition = new SystemStreamPartition(systemStream, partition)

          if (!offsets.contains(systemStreamPartition)) {
            val offsetType = defaultOffsets.getOrElse(systemStream, throw new SamzaException("No default offeset defined for %s. Unable to load a default." format systemStream))

            debug("Got default offset type %s for %s" format (offsetType, systemStreamPartition))

            val systemStreamPartitionMetadata = systemStreamMetadata
              .getSystemStreamPartitionMetadata
              .get(partition)
            val nextOffset = if (systemStreamPartitionMetadata != null) {
              systemStreamPartitionMetadata.getOffset(offsetType)
            } else {
              throw new SamzaException("No metadata available for %s." format systemStreamPartitionMetadata)
            }

            debug("Got next default offset %s for %s" format (nextOffset, systemStreamPartition))

            offsets += systemStreamPartition -> nextOffset
          }
      }
    })
  }
}