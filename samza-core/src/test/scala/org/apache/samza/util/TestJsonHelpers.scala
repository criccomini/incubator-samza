package org.apache.samza.util

import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.job.ShellCommandBuilder
import org.apache.samza.container.TaskNamesToSystemStreamPartitions
import org.apache.samza.Partition
import org.junit.Test
import org.junit.Assert._
import org.apache.samza.container.TaskName
import org.apache.samza.job.ShellCommandBuilder

class TestJsonHelpers {
  @Test
  def testJsonCreateStreamPartitionStringRoundTrip() {
    val getPartitions: Set[SystemStreamPartition] = {
      // Build a heavily skewed set of partitions.
      def partitionSet(max: Int) = (0 until max).map(new Partition(_)).toSet
      val system = "all-same-system."
      val lotsOfParts = Map(system + "topic-with-many-parts-a" -> partitionSet(128),
        system + "topic-with-many-parts-b" -> partitionSet(128), system + "topic-with-many-parts-c" -> partitionSet(64))
      val fewParts = ('c' to 'z').map(l => system + l.toString -> partitionSet(4)).toMap
      val streamsMap = (lotsOfParts ++ fewParts)
      (for (
        s <- streamsMap.keys;
        part <- streamsMap.getOrElse(s, Set.empty)
      ) yield new SystemStreamPartition(Util.getSystemStreamFromNames(s), part)).toSet
    }

    // Group by partition...
    val sspTaskNameMap = TaskNamesToSystemStreamPartitions(getPartitions.groupBy(p => new TaskName(p.getPartition.toString)).toMap)

    val asString = JsonHelpers.serializeSystemStreamPartitionSetToJSON(sspTaskNameMap.getJavaFriendlyType)

    val backFromSSPTaskNameMap = TaskNamesToSystemStreamPartitions(JsonHelpers.deserializeSystemStreamPartitionSetFromJSON(asString))
    assertEquals(sspTaskNameMap, backFromSSPTaskNameMap)
  }
}