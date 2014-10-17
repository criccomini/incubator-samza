package org.apache.samza.container.grouper.task;

import java.util.Map;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public interface TaskGrouper {
  public Map<Integer, Set<TaskName>> group(Map<TaskName, Set<SystemStreamPartition>> tasks);
}
