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

import java.io.File
import java.util.Random
import grizzled.slf4j.Logging
import org.apache.samza.{ Partition, SamzaException }
import org.apache.samza.config.Config
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import scala.collection.JavaConversions._
import org.apache.samza.system.{ SystemStreamPartition, SystemAdmin, SystemFactory, SystemStream }

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
   * corresponding to them all
   *
   * @param config Source of truth for systems and inputStreams
   * @return Set of SystemStreamPartitions, one for each unique system, stream and partition
   */
  def getInputStreamPartitions(config: Config): Set[SystemStreamPartition] = {
    val systemStreams = config.getInputStreams
    val systemNames = config.getSystemNames

    val systemAdmins: Map[String, SystemAdmin] = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      val systemAdmin = systemFactory.getAdmin(systemName, config)
      (systemName, systemAdmin)
    }).toMap

    systemStreams
      .groupBy(_.getSystem)
      .flatMap {
        case (systemName, systemStreamsToGetMetadata) =>
          systemAdmins
            .getOrElse(systemName, throw new IllegalArgumentException("Could not find a stream admin for system '" + systemName + "'"))
            .getSystemStreamMetadata(systemStreamsToGetMetadata.map(_.getStream))
            .flatMap {
              case (streamName, metadata) =>
                metadata
                  .getSystemStreamPartitionMetadata
                  .keys
                  .map(new SystemStreamPartition(systemName, streamName, _))
            }
      }
      .toSet
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
   * For specified containerId, create a list of of the streams and partitions that task should handle,
   * based on the number of tasks in the job
   *
   * @param containerId TaskID to determine work for
   * @param containerCount Total number of tasks in the job
   * @param ssp All SystemStreamPartitions
   * @return Collection of streams and partitions for this particular containerId
   */
  def getStreamsAndPartitionsForContainer(containerId: Int, containerCount: Int, ssp: Set[SystemStreamPartition]): Set[SystemStreamPartition] = {
    ssp.filter(_.getPartition.getPartitionId % containerCount == containerId)
  }

  /**
   * Serialize a collection of stream-partitions to a string suitable for passing between processes.
   * The streams will be grouped by partition. The partition will be separated from the topics by
   * a colon (":"), the topics separated by commas (",") and the topic-stream groups by a slash ("/").
   * Ordering of the grouping is not specified.
   *
   * For example: (A,0),(A,4)(B,0)(B,4)(C,0) could be transformed to: 4:a,b/0:a,b,c
   *
   * @param sp Stream topics to group into a string
   * @return Serialized string of the topics and streams grouped and delimited
   */
  def createStreamPartitionString(sp: Set[SystemStreamPartition]): String = {
    for (
      ch <- List(':', ',', '/');
      s <- sp
    ) {
      if (s.getStream.contains(ch)) throw new IllegalArgumentException(s + " contains illegal character " + ch)
    }

    sp.groupBy(_.getPartition).map(z => z._1.getPartitionId + ":" + z._2.map(y => y.getSystem + "." + y.getStream).mkString(",")).mkString("/")

  }

  /**
   * Invert @{list createStreamPartitionString}, building a list of streams and their partitions,
   * from the string that function produced.
   *
   * @param sp Strings and partitions encoded as a stream by the above function
   * @return List of string and partition tuples extracted from string. Order is not necessarily preserved.
   */
  def createStreamPartitionsFromString(sp: String): Set[SystemStreamPartition] = {
    if (sp == null || sp.isEmpty) return Set.empty

    def splitPartitionGroup(pg: String) = {
      val split = pg.split(":") // Seems like there should be a more scalar way of doing this
      val part = split(0).toInt
      val streams = split(1).split(",").toList

      streams.map(s => new SystemStreamPartition(getSystemStreamFromNames(s), new Partition(part))).toSet
    }

    sp.split("/").map(splitPartitionGroup(_)).toSet.flatten
  }

  /**
   * Makes sure that an object is not null, and throws a NullPointerException
   * if it is.
   */
  def notNull[T](obj: T, msg: String) = if (obj == null) {
    throw new NullPointerException(msg)
  }
}
