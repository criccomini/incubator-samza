package org.apache.samza.container

import org.apache.samza.checkpoint.CheckpointManager
import org.apache.samza.system.SystemAdmin
import org.apache.samza.Partition
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.checkpoint.Checkpoint
import scala.collection.JavaConversions._
import org.apache.samza.SamzaException
import grizzled.slf4j.Logging

// TODO where does this file belong?

class OffsetManager(
  systemAdmins: Map[String, SystemAdmin] = Map(),
  checkpointManager: CheckpointManager = null,
  defaultOffsets: Map[SystemStream, String] = Map(),
  resetOffsets: Map[SystemStream, Boolean] = Map()) extends Logging {

  var lastProcessedOffsets = Map[SystemStreamPartition, String]()

  def start {
    if (checkpointManager != null) {
      checkpointManager.start

      // Load checkpoints
      lastProcessedOffsets
        .keys
        .map(_.getPartition)
        .toSet
        .flatMap(restoreOffsetsFromCheckpoint)
    }

    // Filter checkpoints not in inputStreams
    // Filter checkpoints that have a forced reset
    // Convert checkpoints from lastProcessedOffset to nextOffset
    // Combine checkpoint offsets with default offsets
  }

  def register(partition: Partition) {
    if (checkpointManager != null) {
      debug("Registering partition %s with checkpoint manager." format partition)

      checkpointManager.register(partition)
    } else {
      debug("Skipping partition %s registration with checkpoint manager because no checkpoint manager was supplied." format partition)
    }
  }

  def update(systemStreamPartition: SystemStreamPartition, offset: String) {
    lastProcessedOffsets += systemStreamPartition -> offset
  }

  def next(systemStreamPartition: SystemStreamPartition) = {
    val lastProcessedOffset = offsets.getOrElse(systemStreamPartition)
    val systemAdmin = systemAdmins.getOrElse(systemStreamPartition.getSystemStream)
    systemAdmin.get
  }

  /**
   * Checkpoint all offsets for a partition to the underlying checkpoint manager.
   */
  def checkpoint(partition: Partition) {
    if (checkpointManager != null) {
      debug("Checkpointing offsets for partition %s." format partition)

      val partitionOffsets = lastProcessedOffsets
        .filterKeys(_.getPartition.equals(partition))
        .map { case (systemStreamPartition, offset) => (systemStreamPartition.getSystemStream, offset) }
        .toMap

      checkpointManager.writeCheckpoint(partition, new Checkpoint(partitionOffsets))
    } else {
      debug("Skipping checkpointing for partition %s because no checkpoint manager is defined." format partition)
    }
  }

  def stop() {
    if (checkpointManager != null) {
      debug("Shutting down checkpoint manager.")

      checkpointManager.stop
    } else {
      debug("Skipping checkpoint manager shutdown because no checkpoint manager is defined.")
    }
  }

  private def restoreOffsetsFromCheckpoint(partition: Partition) = {
    checkpointManager
      .readLastCheckpoint(partition)
      .getOffsets
      .map { case (systemStream, offset) => (new SystemStreamPartition(systemStream, partition), offset) }
      .toMap
  }
}