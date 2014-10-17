package org.apache.samza.coordinator.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.container.grouper.task.TaskGrouper;
import org.apache.samza.system.SystemStreamPartition;

public class SamzaJobModel {
  private final Set<SamzaContainerModel> containers;

  public SamzaJobModel(Set<SamzaContainerModel> containers) {
    this.containers = Collections.unmodifiableSet(containers);
  }

  public Set<SamzaContainerModel> getContainers() {
    return containers;
  }

  // TODO figure out how to refactor this. potentially split into the
  // appropriate model classes.
  public static SamzaJobModel getJobModel(SystemStreamPartitionGrouper systemStreamPartitionGrouper, TaskGrouper taskGrouper, Set<SystemStreamPartition> systemStreamPartitions, int containerCount, Map<TaskName, Partition> taskToChangelogPartitionMapping) {
    Map<TaskName, Set<SystemStreamPartition>> taskToSystemStreamPartitionMapping = systemStreamPartitionGrouper.group(systemStreamPartitions);
    Map<Integer, Set<TaskName>> containerToTaskMapping = taskGrouper.group(taskToSystemStreamPartitionMapping);
    Set<SamzaContainerModel> containers = new HashSet<SamzaContainerModel>();
    int maxChangelogPartitionId = 0;

    if (taskToChangelogPartitionMapping.size() > 0) {
      ArrayList<Partition> sortedList = new ArrayList<Partition>(taskToChangelogPartitionMapping.values());
      Collections.sort(sortedList);
      maxChangelogPartitionId = sortedList.get(sortedList.size() - 1).getPartitionId() + 1;
    }

    for (Map.Entry<Integer, Set<TaskName>> containerToTaskEntry : containerToTaskMapping.entrySet()) {
      int id = containerToTaskEntry.getKey();
      Set<SamzaTaskModel> tasks = new HashSet<SamzaTaskModel>();

      for (TaskName taskName : containerToTaskEntry.getValue()) {
        Partition taskChangelogPartition = taskToChangelogPartitionMapping.get(taskName);

        if (taskChangelogPartition == null) {
          taskChangelogPartition = new Partition(maxChangelogPartitionId);
          ++maxChangelogPartitionId;
        }

        tasks.add(new SamzaTaskModel(taskName, taskToSystemStreamPartitionMapping.get(taskName), taskChangelogPartition));
      }

      new SamzaContainerModel(id, tasks);
    }

    return new SamzaJobModel(containers);
  }
}
