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

package org.apache.samza.job

import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.util.Util
import org.apache.samza.util.CommandLine
import org.apache.samza.util.Logging
import scala.collection.JavaConversions._
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.ConfigException
import org.apache.samza.config.SystemConfig
import org.apache.samza.system.SystemFactory
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.job.coordinator.stream.CoordinatorStreamMessage
import org.apache.samza.job.coordinator.stream.CoordinatorStreamSystemProducer
import org.apache.samza.system.SystemStream

object JobRunner {
  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    new JobRunner(config).run
  }
}

/**
 * ConfigRunner is a helper class that sets up and executes a Samza job based
 * on a config URI. The configFactory is instantiated, fed the configPath,
 * and returns a Config, which is used to execute the job.
 */
class JobRunner(config: Config) extends Logging with Runnable {
  def run() {
    val coordinatorSystemName = writeConfig(config)

    val jobFactoryClass = config.getStreamJobFactoryClass match {
      case Some(factoryClass) => factoryClass
      case _ => throw new SamzaException("no job factory class defined")
    }

    val jobFactory = Class.forName(jobFactoryClass).newInstance.asInstanceOf[StreamJobFactory]

    info("job factory: %s" format (jobFactoryClass))
    debug("config: %s" format (config))

    // Create the actual job, and submit it.
    val job = jobFactory.getJob(config).submit

    info("waiting for job to start")

    // Wait until the job has started, then exit.
    Option(job.waitForStatus(Running, 500)) match {
      case Some(appStatus) => {
        if (Running.equals(appStatus)) {
          info("job started successfully")
        } else {
          warn("unable to start job successfully. job has status %s" format (appStatus))
        }
      }
      case _ => warn("unable to start job successfully.")
    }

    info("exiting")
  }

  // TODO clean this up.
  def writeConfig(config: Config) = {
    val systemName = config.getCoordinatorSystemName
    val jobName = config.getName.getOrElse(throw new ConfigException("Missing required config: job.name"))
    val jobId = config.getJobId.getOrElse("1")
    val streamName = Util.getCoordinatorStreamName(jobName, jobId)
    val coordinatorSystemStream = new SystemStream(systemName, streamName)
    val systemFactoryClassName = config.getSystemFactory(systemName).getOrElse("Missing " + SystemConfig.SYSTEM_FACTORY format systemName + " configuration.")
    val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
    val systemProducer = systemFactory.getProducer(systemName, config, new MetricsRegistryMap)
    val source = "job-runner" // TODO
    val coordinatorSystemProducer = new CoordinatorStreamSystemProducer(coordinatorSystemStream, systemProducer)
    // TODO create the topic if it doesn't exist.
    systemProducer.register(source)
    systemProducer.start
    config.foreach {
      case (k, v) =>
        coordinatorSystemProducer.send(new CoordinatorStreamMessage.SetConfig(source, k, v))
    }
    systemProducer.stop
    systemName
  }
}
