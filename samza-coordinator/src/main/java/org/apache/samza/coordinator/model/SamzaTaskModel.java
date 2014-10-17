package org.apache.samza.coordinator.model;

import java.util.Collections;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public class SamzaTaskModel {
  private final Set<SystemStreamPartition> systemStreamPartitions;
  private final Partition changelogPartition;
  private final TaskName taskName;

  public SamzaTaskModel(TaskName taskName, Set<SystemStreamPartition> systemStreamPartitions, Partition changelogPartition) {
    this.taskName = taskName;
    this.systemStreamPartitions = Collections.unmodifiableSet(systemStreamPartitions);
    this.changelogPartition = changelogPartition;
  }

  public Set<SystemStreamPartition> getSystemStreamPartitions() {
    return systemStreamPartitions;
  }

  public Partition getChangelogPartition() {
    return changelogPartition;
  }

  public TaskName getTaskName() {
    return taskName;
  }
}
