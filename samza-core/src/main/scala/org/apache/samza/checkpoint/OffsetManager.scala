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
import org.apache.samza.config.Config
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SystemConfig.Config2System

/**
 * OffsetSetting encapsulates a SystemStream's metadata, default offset, and
 * reset offset settings. It's just a convenience class to make OffsetManager
 * easier to work with.
 */
case class OffsetSetting(
  /**
   * The metadata for the SystemStream.
   */
  metadata: SystemStreamMetadata,

  /**
   * The default offset (oldest, newest, or upcoming) for the SystemStream.
   * This setting is used when no checkpoint is available for a SystemStream
   * if the job is starting for the first time, or the SystemStream has been
   * reset (see resetOffsets, below).
   */
  defaultOffset: OffsetType,

  /**
   * Whether the SystemStream's offset should be reset or not. Determines
   * whether an offset should be ignored at initialization time, even if a
   * checkpoint is available. This is useful for jobs that wish to restart
   * reading from a stream at a different position than where they last
   * checkpointed. If this is true, then defaultOffset will be used to find
   * the new starting position in the stream.
   */
  resetOffset: Boolean)

/**
 * OffsetManager object is a helper that does wiring to build an OffsetManager
 * from a config object.
 */
object OffsetManager extends Logging {
  def apply(systemStreamMetadata: Map[SystemStream, SystemStreamMetadata], config: Config, checkpointManager: CheckpointManager = null) = {
    debug("Building offset manager for %s." format systemStreamMetadata)
    val offsetSettings = systemStreamMetadata
      .map {
        case (systemStream, systemStreamMetadata) =>
          // Get default offset.
          val streamDefaultOffset = config.getDefaultStreamOffset(systemStream)
          val systemDefaultOffset = config.getDefaultSystemOffset(systemStream.getSystem)
          val defaultOffsetType = if (streamDefaultOffset.isDefined) {
            OffsetType.valueOf(streamDefaultOffset.get.toUpperCase)
          } else if (systemDefaultOffset.isDefined) {
            OffsetType.valueOf(systemDefaultOffset.get.toUpperCase)
          } else {
            debug("No default offset for %s defined. Using newest." format systemStream)
            OffsetType.UPCOMING
          }
          debug("Using default offset %s for %s." format (defaultOffsetType, systemStream))

          // Get reset offset.
          val resetOffset = config.getResetOffset(systemStream)
          debug("Using reset offset %s for %s." format (resetOffset, systemStream))

          // Build OffsetSetting so we can create a map for OffsetManager.
          (systemStream, OffsetSetting(systemStreamMetadata, defaultOffsetType, resetOffset))
      }.toMap
    new OffsetManager(offsetSettings, checkpointManager)
  }
}

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
   * Offset settings for all streams that the OffsetManager is managing.
   */
  val offsetSettings: Map[SystemStream, OffsetSetting] = Map(),

  /**
   * Optional checkpoint manager for checkpointing next offsets whenever
   * checkpoint is called.
   */
  val checkpointManager: CheckpointManager = null) extends Logging {

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
        val systemStream = systemStreamPartition.getSystemStream
        val offsetSetting = offsetSettings.getOrElse(systemStream, throw new SamzaException("Attempting to reset a stream that doesn't have offset settings %s." format systemStream))

        if (offsetSetting.resetOffset) {
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
        val offsetSetting = offsetSettings.getOrElse(systemStream, throw new SamzaException("Attempting to load defaults for stream %s, which has no offset settings." format systemStream))
        val systemStreamMetadata = offsetSetting.metadata
        val offsetType = offsetSetting.defaultOffset

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
