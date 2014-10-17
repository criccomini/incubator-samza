package org.apache.samza.container.grouper.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

/**
 * Group the SSP taskNames by dividing the number of taskNames into the number
 * of containers (n) and assigning n taskNames to each container as returned by
 * iterating over the keys in the map of taskNames (whatever that ordering
 * happens to be). No consideration is given towards locality, even distribution
 * of aggregate SSPs within a container, even distribution of the number of
 * taskNames between containers, etc.
 */
public class GroupByContainerCount implements TaskGrouper {
  private final int numContainers;

  public GroupByContainerCount(int numContainers) {
    this.numContainers = numContainers;
  }

  @Override
  public Map<Integer, Set<TaskName>> group(Map<TaskName, Set<SystemStreamPartition>> tasks) {
    Map<Integer, Set<TaskName>> groupedTasks = new HashMap<Integer, Set<TaskName>>();
    int keySize = tasks.keySet().size();
    ArrayList<TaskName> sortedTasks = new ArrayList<TaskName>(tasks.keySet());
    Collections.sort(sortedTasks);

    for (int i = 0; i < keySize; ++i) {
      int index = i % numContainers;
      Set<TaskName> taskSet = groupedTasks.get(index);

      if (taskSet == null) {
        taskSet = new HashSet<TaskName>();
        groupedTasks.put(index, taskSet);
      }

      taskSet.add(sortedTasks.get(i));
    }

    return groupedTasks;
  }
}
