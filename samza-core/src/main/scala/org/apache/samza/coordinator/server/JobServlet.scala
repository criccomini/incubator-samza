package org.apache.samza.coordinator.server

import org.apache.samza.config.Config
import java.util.HashMap
import org.apache.samza.container.TaskNamesToSystemStreamPartitions
import org.apache.samza.util.JsonHelpers
import org.apache.samza.container.TaskName

class JobServlet(
  config: Config,
  containerToTaskMapping: Map[Int, TaskNamesToSystemStreamPartitions],
  taskToChangelogMapping: Map[TaskName, Int]) extends ServletBase {
  import JsonHelpers._

  val javaSafeContainerToTaskMapping = buildTasksToSSPs
  val javaSafeTaskToChangelogMappings = convertTaskNameToChangeLogPartitionMapping(taskToChangelogMapping)
  val jsonMap = buildJsonMap

  protected def getObjectToWrite() = {
    jsonMap
  }

  private def buildTasksToSSPs = {
    containerToTaskMapping.mapValues {
      case (taskNameToSSPs) =>
        convertSystemStreamPartitionSetToJSON(taskNameToSSPs.getJavaFriendlyType)
    }
  }

  private def buildJsonMap = {
    val map = new HashMap[String, Object]()
    map.put("config", config)
    map.put("containers", javaSafeContainerToTaskMapping)
    map.put("task-changelog-mappings", javaSafeTaskToChangelogMappings)
    map
  }
}
