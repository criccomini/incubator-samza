package org.apache.samza.util

import org.apache.samza.container.TaskName
import org.codehaus.jackson.map.ObjectMapper
import org.apache.samza.system.SystemStreamPartition
import org.codehaus.jackson.`type`.TypeReference
import java.util
import scala.collection.JavaConversions._
import org.apache.samza.Partition
import scala.reflect.BeanProperty
import org.apache.samza.config.MapConfig
import org.apache.samza.container.TaskNamesToSystemStreamPartitions

object JsonHelpers {
  // Jackson really hates Scala's classes, so we need to wrap up the SSP in a form Jackson will take.
  class SSPWrapper(@BeanProperty var partition: java.lang.Integer = null,
    @BeanProperty var Stream: java.lang.String = null,
    @BeanProperty var System: java.lang.String = null) {
    def this() { this(null, null, null) }
    def this(ssp: SystemStreamPartition) { this(ssp.getPartition.getPartitionId, ssp.getSystemStream.getStream, ssp.getSystemStream.getSystem) }
  }

  def convertSystemStreamPartitionSetToJSON(sspTaskNames: java.util.Map[TaskName, java.util.Set[SystemStreamPartition]]): util.HashMap[TaskName, util.ArrayList[SSPWrapper]] = {
    val map = new util.HashMap[TaskName, util.ArrayList[SSPWrapper]]()
    for ((key, ssps) <- sspTaskNames) {
      val al = new util.ArrayList[SSPWrapper](ssps.size)
      for (ssp <- ssps) { al.add(new SSPWrapper(ssp)) }
      map.put(key, al)
    }
    map
  }

  def serializeSystemStreamPartitionSetToJSON(sspTaskNames: java.util.Map[TaskName, java.util.Set[SystemStreamPartition]]) = {
    new ObjectMapper().writeValueAsString(convertSystemStreamPartitionSetToJSON(sspTaskNames))
  }

  def deserializeSystemStreamPartitionSetFromJSON(sspsAsJSON: String): Map[TaskName, Set[SystemStreamPartition]] = {
    val om = new ObjectMapper()
    val asWrapper = om.readValue(sspsAsJSON, new TypeReference[util.HashMap[String, util.ArrayList[SSPWrapper]]]() {}).asInstanceOf[util.HashMap[String, util.ArrayList[SSPWrapper]]]
    val taskName = for (
      (key, sspsWrappers) <- asWrapper;
      taskName = new TaskName(key);
      ssps = sspsWrappers.map(w => new SystemStreamPartition(w.getSystem, w.getStream, new Partition(w.getPartition))).toSet
    ) yield (taskName -> ssps)
    taskName.toMap // to get an immutable map rather than mutable...
  }

  def convertTaskNameToChangeLogPartitionMapping(mapping: Map[TaskName, Int]): util.HashMap[TaskName, java.lang.Integer] = {
    val javaMap = new util.HashMap[TaskName, java.lang.Integer]()
    mapping.foreach(kv => javaMap.put(kv._1, Integer.valueOf(kv._2)))
    javaMap
  }

  def serializeTaskNameToChangeLogPartitionMapping(mapping: Map[TaskName, Int]) = {
    new ObjectMapper().writeValueAsString(convertTaskNameToChangeLogPartitionMapping(mapping))
  }

  def deserializeTaskNameToChangeLogPartitionMapping(taskNamesAsJSON: String): Map[TaskName, Int] = {
    val om = new ObjectMapper()
    val asMap = om.readValue(taskNamesAsJSON, new TypeReference[util.HashMap[String, java.lang.Integer]] {}).asInstanceOf[util.HashMap[String, java.lang.Integer]]
    asMap.map(kv => new TaskName(kv._1) -> kv._2.intValue()).toMap
  }

  def deserializeCoordinatorBody(body: String) = new ObjectMapper().readValue(body, new TypeReference[util.HashMap[String, Object]] {}).asInstanceOf[util.HashMap[String, Object]]

  def convertCoordinatorConfig(config: util.Map[String, String]) = new MapConfig(config)

  def convertCoordinatorTaskNameChangelogPartitions(taskNameToChangelogMapping: util.Map[String, java.lang.Integer]) = {
    taskNameToChangelogMapping.map {
      case (taskName, changelogPartitionId) =>
        (new TaskName(taskName), changelogPartitionId.toInt)
    }.toMap
  }

  // First key is containerId, second key is TaskName, third key is [system|stream|partition].
  def convertCoordinatorSSPTaskNames(containers: util.Map[String, util.Map[String, util.List[util.Map[String, Object]]]]): Map[Int, TaskNamesToSystemStreamPartitions] = {
    containers.map {
      case (containerId, tasks) => {
        containerId.toInt -> new TaskNamesToSystemStreamPartitions(tasks.map {
          case (taskName, ssps) => {
            new TaskName(taskName) -> ssps.map {
              case (sspMap) => new SystemStreamPartition(
                sspMap.get("system").toString,
                sspMap.get("stream").toString,
                new Partition(sspMap.get("partition").toString.toInt))
            }.toSet
          }
        }.toMap)
      }
    }.toMap
  }
}