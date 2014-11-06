package org.apache.samza.coordinator.stream

import org.apache.samza.config.Config
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.ConfigException
import org.apache.samza.config.SystemConfig
import org.apache.samza.job.coordinator.stream.CoordinatorStreamSystemProducer
import org.apache.samza.system.SystemStream
import org.apache.samza.util.Util
import org.apache.samza.system.SystemFactory
import org.apache.samza.metrics.MetricsRegistryMap

class CoordinatorStreamSystemFactory {
  def getCoordinatorStreamSystemProducer(config: Config) = {
    val systemName = config.getCoordinatorSystemName
    val jobName = config.getName.getOrElse(throw new ConfigException("Missing required config: job.name"))
    val jobId = config.getJobId.getOrElse("1")
    val streamName = Util.getCoordinatorStreamName(jobName, jobId)
    val coordinatorSystemStream = new SystemStream(systemName, streamName)
    val systemFactoryClassName = config.getSystemFactory(systemName).getOrElse("Missing " + SystemConfig.SYSTEM_FACTORY format systemName + " configuration.")
    val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
    val systemProducer = systemFactory.getProducer(systemName, config, new MetricsRegistryMap)
    val systemAdmin = systemFactory.getAdmin(systemName, config)
    new CoordinatorStreamSystemProducer(coordinatorSystemStream, systemProducer, systemAdmin)
  }
}