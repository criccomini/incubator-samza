package org.apache.samza.coordinator.model;

import java.util.Collections;
import java.util.Set;

public class SamzaModelContainer {
  private final int id;
  private final Set<SamzaModelTask> tasks;

  public SamzaModelContainer(int id, Set<SamzaModelTask> tasks) {
    this.id = id;
    this.tasks = Collections.unmodifiableSet(tasks);
  }

  public int getId() {
    return id;
  }

  public Set<SamzaModelTask> getTasks() {
    return tasks;
  }
}
