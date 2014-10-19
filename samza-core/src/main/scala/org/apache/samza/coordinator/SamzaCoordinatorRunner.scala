package org.apache.samza.coordinator

import org.apache.samza.config.Config
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.server.ServletTaskMapping
import org.apache.samza.coordinator.server.ServletConfig
import org.apache.samza.coordinator.model.SamzaModelJob
import org.apache.samza.util.Util
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import org.apache.samza.system.SystemStreamPartition
import scala.collection.JavaConversions._
import org.apache.samza.util.Logging
import org.apache.samza.config.JobConfig._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper
import org.apache.samza.container.grouper.task.TaskGrouper
import org.apache.samza.Partition
import java.util.Collections
import org.apache.samza.coordinator.model.SamzaModelContainer
import org.apache.samza.coordinator.model.SamzaModelTask
import org.apache.samza.config.ConfigException
import org.apache.samza.job.StreamJobFactory
import org.apache.samza.config.MapConfig
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.container.grouper.task.GroupByContainerCount

object SamzaCoordinatorRunner extends Logging {
  def main(args: Array[String]) {
    val config = new MapConfig(JsonConfigSerializer.fromJson(System.getenv(ShellCommandConfig.ENV_CONFIG)))
    
    SamzaCoordinatorRunner(config).run
  }

  def apply(config: Config) = {
    val systemStreamPartitions = Util.getInputStreamPartitions(config)
    val systemStreamPartitionGrouper = getSystemStreamPartitionGrouper(config)
    val taskGrouper = getTaskGrouper(config)
    val jobModel = getJobModel(systemStreamPartitionGrouper, taskGrouper, systemStreamPartitions, null)
    val scheduler = getSamzaCoordinatorScheduler(config)
    val server = getHttpServer(jobModel, config)
    new SamzaCoordinator(jobModel, scheduler, server)
  }

  def getSystemStreamPartitionGrouper(config: Config) = {
    val factoryString = config.getSystemStreamPartitionGrouperFactory
    val factory = Util.getObj[SystemStreamPartitionGrouperFactory](factoryString)
    factory.getSystemStreamPartitionGrouper(config)
  }

  def getTaskGrouper(config: Config) = {
    val containerCount = config
      .getContainerCount
      .getOrElse("1")
      .toInt
    new GroupByContainerCount(containerCount)
  }

  def getJobModel(
    systemStreamPartitionGrouper: SystemStreamPartitionGrouper,
    taskGrouper: TaskGrouper,
    systemStreamPartitions: Set[SystemStreamPartition],
    taskToChangelogPartitionMapping: Map[TaskName, Partition]) = {

    val taskToSystemStreamPartitionMapping = systemStreamPartitionGrouper.group(systemStreamPartitions);
    val containerToTaskMapping = taskGrouper.group(taskToSystemStreamPartitionMapping);
    var maxChangelogPartitionId = -1;

    if (taskToChangelogPartitionMapping.size() > 0) {
      // Get the largest used partition ID.
      maxChangelogPartitionId = taskToChangelogPartitionMapping
        .values
        .toList
        .distinct
        .sorted
        .last
        .getPartitionId
    }

    val containers = containerToTaskMapping.map {
      case (containerId, taskNames) =>
        val tasks = taskNames.map {
          case (taskName) =>
            val taskChangelogPartition = taskToChangelogPartitionMapping.getOrElse(taskName, {
              maxChangelogPartitionId += 1
              new Partition(maxChangelogPartitionId)
            })
            new SamzaModelTask(taskName, taskToSystemStreamPartitionMapping(taskName), taskChangelogPartition)
        }
        new SamzaModelContainer(containerId, tasks)
    }.toSet

    new SamzaModelJob(containers);
  }

  def getSamzaCoordinatorScheduler(config: Config) = {
    val factoryString = config
      .getStreamJobFactoryClass
      .getOrElse(throw new ConfigException("Missing config for %s." format STREAM_JOB_FACTORY_CLASS))
    val factory = Util.getObj[StreamJobFactory](factoryString)
    factory.getScheduler(config)
  }

  def getHttpServer(jobModel: SamzaModelJob, config: Config) = {
    val server = new HttpServer
    server.addServlet("/config/*", new ServletConfig(config))
    server.addServlet("/tasks/*", new ServletTaskMapping(jobModel))
    server
  }
}
