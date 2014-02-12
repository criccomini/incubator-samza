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

package org.apache.samza.storage

import java.io.File
import scala.collection.Map
import grizzled.slf4j.Logging
import org.apache.samza.Partition
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemStreamPartitionIterator
import org.apache.samza.task.MessageCollector
import org.apache.samza.util.Util
import org.apache.samza.system.SystemAdmin
import org.apache.samza.SamzaException

object TaskStorageManager {
  def getStoreDir(storeBaseDir: File, storeName: String) = {
    new File(storeBaseDir, storeName)
  }

  def getStorePartitionDir(storeBaseDir: File, storeName: String, partition: Partition) = {
    new File(storeBaseDir, storeName + File.separator + partition.getPartitionId)
  }
}

/**
 * Manage all the storage engines for a given task
 */
class TaskStorageManager(
  partition: Partition,
  taskStores: Map[String, StorageEngine] = Map(),
  storeConsumers: Map[String, SystemConsumer] = Map(),
  changeLogSystemStreams: Map[String, SystemStream] = Map(),
  changeLogOffsets: Map[String, String] = Map(),
  storeBaseDir: File = new File(System.getProperty("user.dir"), "state")) extends Logging {

  def apply(storageEngineName: String) = taskStores(storageEngineName)

  def init(collector: MessageCollector) {
    cleanBaseDirs
    startConsumers
    restoreStores(collector)
    stopConsumers
  }

  private def cleanBaseDirs {
    debug("Cleaning base directories for stores.")

    taskStores.keys.foreach(storeName => {
      val storagePartitionDir = TaskStorageManager.getStorePartitionDir(storeBaseDir, storeName, partition)

      debug("Cleaning %s for store %s." format (storagePartitionDir, storeName))

      Util.rm(storagePartitionDir)
      storagePartitionDir.mkdirs
    })
  }

  private def startConsumers {
    debug("Starting consumers for stores.")

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
      val consumer = storeConsumers.getOrElse(storeName, throw new SamzaException("Unable to find a store consumer for store %s." format storeName))

      changeLogOffsets.get(storeName) match {
        case Some(offset) =>
          debug("Registering consumer for system stream partition %s with offset %s." format (systemStreamPartition, offset))

          consumer.register(systemStreamPartition, offset)
        case _ =>
          debug("Skipping consumer setup for changelog %s, since no offset was available to consume from." format systemStreamPartition)
      }
    }

    storeConsumers.values.foreach(_.start)
  }

  private def restoreStores(collector: MessageCollector) {
    debug("Restoring stores.")

    for ((storeName, store) <- taskStores) {
      if (!changeLogSystemStreams.contains(storeName)) {
        info("Skipping changelog restoration for store %s because it is not configured to use a changelog." format storeName)
      } else if (!changeLogOffsets.contains(storeName)) {
        info("Skipping changelog restoration for store %s because no offset was available to begin reading from the changelog." format storeName)
      } else {
        val systemStream = changeLogSystemStreams(storeName)
        val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
        val systemConsumer = storeConsumers(storeName)
        val systemConsumerIterator = new SystemStreamPartitionIterator(systemConsumer, systemStreamPartition);
        store.restore(systemConsumerIterator)
      }
    }
  }

  private def stopConsumers {
    debug("Stopping consumers for stores.")

    storeConsumers.values.foreach(_.stop)
  }

  def flush() {
    debug("Flushing stores.")

    taskStores.values.foreach(_.flush)
  }

  def stop() {
    debug("Stopping stores.")

    taskStores.values.foreach(_.stop)
  }
}
