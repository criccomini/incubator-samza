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

package org.apache.samza.coordinator.stream

import org.apache.samza.config.Config
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.ConfigException
import org.apache.samza.config.SystemConfig
import org.apache.samza.system.SystemStream
import org.apache.samza.util.Util
import org.apache.samza.system.SystemFactory
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.SamzaException
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.apache.samza.system.SystemStreamPartitionIterator
import org.apache.samza.Partition
import scala.collection.JavaConversions._
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetConfig
import org.apache.samza.system.SystemAdmin

/**
 * A helper class that does wiring for CoordinatorStreamSystemConsumer and
 * CoordinatorStreamSystemProducer. This factory should only be used in
 * situations where the underlying SystemConsumer/SystemProducer does not
 * exist.
 */
class CoordinatorStreamSystemFactory {
  def getCoordinatorStreamSystemConsumer(config: Config, registry: MetricsRegistry) = {
    val (coordinatorSystemStream, systemFactory) = getCoordinatorSystemStreamAndFactory(config, registry)
    val systemAdmin = systemFactory.getAdmin(coordinatorSystemStream.getSystem, config)
    val systemConsumer = systemFactory.getConsumer(coordinatorSystemStream.getSystem, config, registry)
    new CoordinatorStreamSystemConsumer(coordinatorSystemStream, systemConsumer, systemAdmin)
  }

  def getCoordinatorStreamSystemProducer(config: Config, registry: MetricsRegistry) = {
    val (coordinatorSystemStream, systemFactory) = getCoordinatorSystemStreamAndFactory(config, registry)
    val systemAdmin = systemFactory.getAdmin(coordinatorSystemStream.getSystem, config)
    val systemProducer = systemFactory.getProducer(coordinatorSystemStream.getSystem, config, registry)
    new CoordinatorStreamSystemProducer(coordinatorSystemStream, systemProducer, systemAdmin)
  }

  private def getCoordinatorSystemStreamAndFactory(config: Config, registry: MetricsRegistry) = {
    val systemName = config.getCoordinatorSystemName
    val (jobName, jobId) = Util.getJobNameAndId(config)
    val streamName = Util.getCoordinatorStreamName(jobName, jobId)
    val coordinatorSystemStream = new SystemStream(systemName, streamName)
    val systemFactoryClassName = config
      .getSystemFactory(systemName)
      .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY format systemName))
    val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
    (coordinatorSystemStream, systemFactory)
  }
}