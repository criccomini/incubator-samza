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

import org.apache.samza.system.SystemStream
import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.OffsetType
import org.apache.samza.SamzaException
import scala.collection.JavaConversions._
import grizzled.slf4j.Logging

/**
 * OffsetManager does three things:
 *
 * <ul>
 * <li>1. Loads initial offsets for all input SystemStreamPartitions in a
 * SamzaContainer.</li>
 * <li>Keep track of the next offset to read for each
 * SystemStreamPartitions in a SamzaContainer.</li>
 * <li>Checkpoint the next offset to read SystemStreamPartitions in a
 * SamzaContainer periodically to the CheckpointManager.</li>
 * </ul>
 *
 * All partitions must be registered before start is called, and start must be
 * called before get/update/checkpoint/stop are called.
 */
class OffsetManager(
  /**
   * Metadata for all streams that the offset manager is tracking.
   */
  streamMetadata: Map[SystemStream, SystemStreamMetadata] = Map(),

  /**
   * Default offset types (oldest, newest, or upcoming) for all streams that
   * the offset manager is tracking. This setting is used when no checkpoint
   * is available for a SystemStream if the job is starting for the first
   * time, or the SystemStream has been reset (see resetOffsets, below).
   */
  defaultOffsets: Map[SystemStream, OffsetType] = Map(),

  /**
   * A set of SystemStreams whose offsets should be ignored at initialization
   * time, even if a checkpoint is available. This is useful for jobs that
   * wish to restart reading from a stream at a different position than where
   * they last checkpointed. If a SystemStream is in this set, its
   * defaultOffset will be used to find the new starting position in the
   * stream.
   */
  resetOffsets: Set[SystemStream] = Set(),

  /**
   * Optional checkpoint manager for checkpointing next offsets whenever
   * checkpoint is called.
   */
  checkpointManager: CheckpointManager = null) extends Logging {

  /**
   * Next offsets to be read for each SystemStreamPartition.
   */
  var offsets = Map[SystemStreamPartition, String]()

  /**
   * The set of system stream partitions that have been registered with the
   * OffsetManager. These are the SSPs that will be tracked within the offset
   * manager.
   */
  var systemStreamPartitions = Set[SystemStreamPartition]()

  /**
   * A derivative of systemStreamPartitions that holds all partitions that
   * have been registered.
   */
  var partitions = Set[Partition]()

  /**
   * Get the next offset to be read for a SystemStreamPartition.
   */
  def apply(systemStreamPartition: SystemStreamPartition) = {
    offsets.get(systemStreamPartition)
  }

  def register(systemStreamPartition: SystemStreamPartition) {
    systemStreamPartitions += systemStreamPartition
    partitions += systemStreamPartition.getPartition
  }

  def start {
    registerCheckpointManager
    loadOffsetsFromCheckpointManager
    stripResetStreams
    loadDefaults

    info("Successfully loaded offsets: %s" format offsets)
  }

  /**
   * Set a new next offset for a given SystemStreamPartition.
   */
  def update(systemStreamPartition: SystemStreamPartition, nextOffset: String) {
    offsets += systemStreamPartition -> nextOffset
  }

  /**
   * Checkpoint all next offsets for a given partition using the CheckpointManager.
   */
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

  /**
   * Register all partitions with the CheckpointManager.
   */
  private def registerCheckpointManager {
    if (checkpointManager != null) {
      debug("Registering checkpoint manager.")

      partitions.foreach(checkpointManager.register)
    } else {
      debug("Skipping checkpoint manager registration because no manager was defined.")
    }
  }

  /**
   * Loads innitial next offsets for all registered partitions.
   */
  private def loadOffsetsFromCheckpointManager {
    if (checkpointManager != null) {
      debug("Loading offsets from checkpoint manager.")

      checkpointManager.start

      offsets ++= partitions.flatMap(restoreOffsetsFromCheckpoint(_))
    } else {
      debug("Skipping offset load from checkpoint manager because no manager was defined.")
    }
  }

  /**
   * Loads next offsets for a single partition.
   */
  private def restoreOffsetsFromCheckpoint(partition: Partition) = {
    debug("Loading checkpoints for partition: %s." format partition)

    checkpointManager
      .readLastCheckpoint(partition)
      .getOffsets
      .map { case (systemStream, offset) => (new SystemStreamPartition(systemStream, partition), offset) }
      .toMap
  }

  /**
   * Removes next offset settings for all SystemStreams that are to be
   * forcibly reset using resetOffsets.
   */
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

  /**
   * Use defaultOffsets to get a next offset for every SystemStreamPartition
   * that was registered, but has no offset.
   */
  private def loadDefaults {
    systemStreamPartitions.foreach(systemStreamPartition => {
      if (!offsets.contains(systemStreamPartition)) {
        val systemStream = systemStreamPartition.getSystemStream
        val partition = systemStreamPartition.getPartition
        val systemStreamMetadata = streamMetadata.getOrElse(systemStream, throw new SamzaException("No metadata available for %s. Can't continue without this information." format systemStream))
        val offsetType = defaultOffsets.getOrElse(systemStream, throw new SamzaException("No default offeset defined for %s. Unable to load a default." format systemStream))

        debug("Got default offset type %s for %s" format (offsetType, systemStreamPartition))

        val systemStreamPartitionMetadata = systemStreamMetadata
          .getSystemStreamPartitionMetadata
          .get(partition)

        if (systemStreamPartitionMetadata != null) {
          val nextOffset = systemStreamPartitionMetadata.getOffset(offsetType)

          debug("Got next default offset %s for %s" format (nextOffset, systemStreamPartition))

          offsets += systemStreamPartition -> nextOffset
        } else {
          throw new SamzaException("No metadata available for partition %s." format systemStreamPartitionMetadata)
        }
      }
    })
  }
}