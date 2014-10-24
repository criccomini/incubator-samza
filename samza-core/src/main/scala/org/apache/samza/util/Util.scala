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

package org.apache.samza.util

import java.io._
import java.lang.management.ManagementFactory
import java.util
import java.util.Random
import java.net.URL
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.CheckpointManagerFactory
import org.apache.samza.config.Config
import org.apache.samza.config.StorageConfig.Config2Storage
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.{ SystemStreamPartition, SystemFactory, StreamMetadataCache, SystemStream }
import scala.collection.JavaConversions._
import scala.collection
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import org.apache.samza.container.grouper.task.GroupByContainerCount
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.job.model.JobModel
import org.apache.samza.job.model.TaskModel
import org.apache.samza.Partition

object Util extends Logging {
  val random = new Random

  /**
   * Make an environment variable string safe to pass.
   */
  def envVarEscape(str: String) = str.replace("\"", "\\\"").replace("'", "\\'")

  /**
   * Get a random number >= startInclusive, and < endExclusive.
   */
  def randomBetween(startInclusive: Int, endExclusive: Int) =
    startInclusive + random.nextInt(endExclusive - startInclusive)

  /**
   * Recursively remove a directory (or file), and all sub-directories. Equivalent
   * to rm -rf.
   */
  def rm(file: File) {
    if (file == null) {
      return
    } else if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        for (f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }

  /**
   * Instantiate a class instance from a given className.
   */
  def getObj[T](className: String) = {
    Class
      .forName(className)
      .newInstance
      .asInstanceOf[T]
  }

  /**
   * For each input stream specified in config, exactly determine its partitions, returning a set of SystemStreamPartitions
   * containing them all
   *
   * @param config Source of truth for systems and inputStreams
   * @return Set of SystemStreamPartitions, one for each unique system, stream and partition
   */
  def getInputStreamPartitions(config: Config): Set[SystemStreamPartition] = {
    val inputSystemStreams = config.getInputStreams
    val systemNames = config.getSystemNames.toSet

    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      systemName -> systemFactory.getAdmin(systemName, config)
    }).toMap

    // Get the set of partitions for each SystemStream from the stream metadata
    new StreamMetadataCache(systemAdmins)
      .getStreamMetadata(inputSystemStreams)
      .flatMap {
        case (systemStream, metadata) =>
          metadata
            .getSystemStreamPartitionMetadata
            .keys
            .map(new SystemStreamPartition(systemStream, _))
      }.toSet
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getSystemStreamFromNames(systemStreamNames: String): SystemStream = {
    val idx = systemStreamNames.indexOf('.')
    if (idx < 0)
      throw new IllegalArgumentException("No '.' in stream name '" + systemStreamNames + "'. Stream names should be in the form 'system.stream'")
    new SystemStream(systemStreamNames.substring(0, idx), systemStreamNames.substring(idx + 1, systemStreamNames.length))
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getNameFromSystemStream(systemStream: SystemStream) = {
    systemStream.getSystem + "." + systemStream.getStream
  }

  /**
   * Makes sure that an object is not null, and throws a NullPointerException
   * if it is.
   */
  def notNull[T](obj: T, msg: String) = if (obj == null) {
    throw new NullPointerException(msg)
  }

  /**
   * Returns the name representing the JVM. It usually contains the PID of the process plus some additional information
   * @return String that contains the name representing this JVM
   */
  def getContainerPID(): String = {
    ManagementFactory.getRuntimeMXBean().getName()
  }

  /**
   * Reads a URL and returns its body as a string. Does no error handling.
   *
   * @param url HTTP URL to read from.
   * @return String payload of the body of the HTTP response.
   */
  def read(url: URL): String = {
    val conn = url.openConnection();
    val br = new BufferedReader(new InputStreamReader(conn.getInputStream));
    var line: String = null;
    val body = Iterator.continually(br.readLine()).takeWhile(_ != null).mkString
    br.close
    body
  }

  /**
   * Fetches config, task:SSP assignments, and task:changelog partition
   * assignments, and returns objects to be used for SamzaContainer's
   * constructor.
   */
  def readJobModel(url: String) = {
    info("Fetching configuration from: %s" format url)
    JsonSerializers
      .getObjectMapper
      .readValue(Util.read(new URL(url)), classOf[JobModel])
  }

  // TODO TODO TODO TODO TODO TODO write a unit test for this method.
  
  // TODO containerCount should go away when we generalize the job coordinator, 
  // and have a non-yarn-specific way of specifying container count.
  def buildJobModel(config: Config, containerCount: Int) = {
    // Get checkpoint manager if one is defined.
    val checkpointManager = config.getCheckpointManagerFactory match {
      case Some(checkpointFactoryClassName) =>
        Util
          .getObj[CheckpointManagerFactory](checkpointFactoryClassName)
          .getCheckpointManager(config, new MetricsRegistryMap)
      case _ =>
        if (!config.getStoreNames.isEmpty) {
          throw new SamzaException("Storage factories configured, but no checkpoint manager has been specified.  " +
            "Unable to start job as there would be no place to store changelog partition mapping.")
        }
        null
    }

    // Get previous changelog partition mappings for tasks, if checkpoint 
    // manager exists.
    val previousChangelogeMapping = if (checkpointManager != null) {
      checkpointManager.start
      checkpointManager.readChangeLogPartitionMapping
    } else {
      new util.HashMap[TaskName, java.lang.Integer]()
    }

    // Hold on to the max changelog partition id, so we can assign new tasks a 
    // new changelog partition id.
    var maxChangelogPartitionId = previousChangelogeMapping
      .values
      .map(_.toInt)
      .toList
      .sorted
      .lastOption
      .getOrElse(-1)
    val allSystemStreamPartitions = Util.getInputStreamPartitions(config)

    // Assign all SystemStreamPartitions to TaskNames.
    val taskModels = {
      val factoryString = config.getSystemStreamPartitionGrouperFactory
      val factory = Util.getObj[SystemStreamPartitionGrouperFactory](factoryString)
      val grouper = factory.getSystemStreamPartitionGrouper(config)
      val groups = grouper.group(allSystemStreamPartitions)
      info("SystemStreamPartitionGrouper " + grouper + " has grouped the SystemStreamPartitions into the following taskNames:")
      groups
        .map {
          case (taskName, systemStreamPartitions) =>
            val changelogPartition = Option(previousChangelogeMapping.get(taskName)) match {
              case Some(changelogPartitionId) => new Partition(changelogPartitionId)
              case _ =>
                // If we've never seen this TaskName before, then assign it a 
                // new changelog.
                maxChangelogPartitionId += 1
                info("New task %s is being assigned changelog partition %s." format (taskName, maxChangelogPartitionId))
                new Partition(maxChangelogPartitionId)
            }
            new TaskModel(taskName, systemStreamPartitions, changelogPartition)
        }
        .toSet
    }

    // Save the changelog mapping back to the checkpoint manager.
    if (checkpointManager != null) {
      val newChangelogMapping = taskModels.map {
        case (taskModel) =>
          taskModel.getTaskName -> Integer.valueOf(taskModel.getChangelogPartition.getPartitionId)
      }.toMap
      info("Saving task-to-changelog partition mapping: %s" format newChangelogMapping)
      checkpointManager.writeChangeLogPartitionMapping(newChangelogMapping)
      checkpointManager.stop
    }

    // Here is where we should put in a pluggable option for the 
    // SSPTaskNameGrouper for locality, load-balancing, etc.
    val containerGrouper = new GroupByContainerCount(containerCount)
    val containerModels = containerGrouper
      .group(taskModels)
      .map { case (containerModel) => Integer.valueOf(containerModel.getContainerId) -> containerModel }
      .toMap
    new JobModel(config, containerModels)
  }
}
